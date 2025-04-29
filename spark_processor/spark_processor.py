# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col, current_timestamp, udf
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# # Schema for Kafka message
# schema = StructType([
#     StructField("video_id", StringType(), True),
#     StructField("title", StringType(), True),
#     StructField("channel_id", StringType(), True),
#     StructField("channel_title", StringType(), True),
#     StructField("category_id", StringType(), True),
#     StructField("publish_time", StringType(), True),
#     StructField("view_count", IntegerType(), True),
#     StructField("like_count", IntegerType(), True),
#     StructField("comment_count", IntegerType(), True),
#     StructField("duration", StringType(), True),
#     StructField("tags", ArrayType(StringType()), True),
#     StructField("thumbnail_url", StringType(), True),
#     StructField("fetch_time", StringType(), True)
# ])

# # Category ID to name mapping
# category_mapping = {
#     "1": "Film & Animation", "2": "Autos & Vehicles", "10": "Music", "15": "Pets & Animals",
#     "17": "Sports", "18": "Short Movies", "19": "Travel & Events", "20": "Gaming",
#     "21": "Videoblogging", "22": "People & Blogs", "23": "Comedy", "24": "Entertainment",
#     "25": "News & Politics", "26": "Howto & Style", "27": "Education", "28": "Science & Technology",
#     "29": "Nonprofits & Activism", "30": "Movies", "31": "Anime/Animation", "32": "Action/Adventure",
#     "33": "Classics", "34": "Comedy", "35": "Documentary", "36": "Drama", "37": "Family",
#     "38": "Foreign", "39": "Horror", "40": "Sci-Fi/Fantasy", "41": "Thriller", "42": "Shorts",
#     "43": "Shows", "44": "Trailers"
# }

# @udf(StringType())
# def get_category_name(category_id):
#     return category_mapping.get(category_id, "Unknown")

# def main():
#     spark = SparkSession.builder \
#         .appName("YoutubeIndiaTrendingProcessor") \
#         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
#         .getOrCreate()

#     spark.sparkContext.setLogLevel("WARN")

#     # Read stream from Kafka
#     df = spark.readStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", "kafka:9092") \
#         .option("subscribe", "trending_videos") \
#         .option("startingOffsets", "latest") \
#         .load()

#     # Parse and enrich
#     parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
#     enriched_df = parsed_df \
#         .withColumn("category_name", get_category_name(col("category_id"))) \
#         .withColumn("processing_time", current_timestamp())

#     # Output to console
#     query = enriched_df.writeStream \
#         .outputMode("append") \
#         .format("console") \
#         .option("truncate", "false") \
#         .start()

#     query.awaitTermination()

# if __name__ == "__main__":
#     main()



from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, TimestampType

def create_spark_session(app_name="YouTubeDataAnalyzer"):
    """Create and configure a Spark session for streaming"""
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.shuffle.partitions", "2")  # Reduce for local testing
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")  # Kafka connector
            .getOrCreate())

def define_schema():
    """Define the schema for YouTube video data"""
    return StructType([
        StructField("video_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("channel_id", StringType(), True),
        StructField("channel_title", StringType(), True),
        StructField("category_id", StringType(), True),
        StructField("publish_time", StringType(), True),
        StructField("view_count", IntegerType(), True),
        StructField("like_count", IntegerType(), True),
        StructField("comment_count", IntegerType(), True),
        StructField("duration", StringType(), True),
        StructField("tags", ArrayType(StringType()), True),
        StructField("thumbnail_url", StringType(), True),
        StructField("fetch_time", StringType(), True)
    ])

def read_from_kafka(spark):
    return (spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "kafka:29092")  # Changed from localhost:9092
            .option("subscribe", "trending_videos")
            .option("startingOffsets", "earliest")
            .load())

def process_stream(kafka_df, schema):
    """Process the streaming data"""
    # Parse the value from Kafka as JSON using the defined schema
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Convert string timestamps to proper timestamp type
    df = parsed_df.withColumn("publish_time", col("publish_time").cast(TimestampType()))
    df = df.withColumn("fetch_time", col("fetch_time").cast(TimestampType()))
    
    # Register as a temporary view for SQL queries
    df.createOrReplaceTempView("trending_videos")
    
    return df

def analyze_trending(df, spark):
    """Perform analysis on trending videos"""
    # 1. Top channels by video count in the trending section
    top_channels = (df.groupBy("channel_title")
                    .agg(count("video_id").alias("video_count"))
                    .orderBy(desc("video_count")))
    
    # 2. Average engagement (views, likes, comments) by category
    engagement_by_category = (df.groupBy("category_id")
                             .agg(
                                 avg("view_count").alias("avg_views"),
                                 avg("like_count").alias("avg_likes"),
                                 avg("comment_count").alias("avg_comments")
                             ))
    
    # Return streaming dataframes for output
    return top_channels, engagement_by_category

def start_streaming_queries(top_channels_df, engagement_df):
    """Start the streaming queries and output to console"""
    # Output top channels to console
    query1 = (top_channels_df
              .writeStream
              .outputMode("complete")  # Show all results each time
              .format("console")
              .option("truncate", "false")
              .start())
    
    # Output engagement metrics to console
    query2 = (engagement_df
              .writeStream
              .outputMode("complete")
              .format("console")
              .option("truncate", "false")
              .start())
    
    return [query1, query2]

def main():
    """Main function to run the Spark Streaming application"""
    # Create Spark session
    spark = create_spark_session()
    
    # Define schema for the YouTube data
    schema = define_schema()
    
    # Read from Kafka
    kafka_df = read_from_kafka(spark)
    
    # Process the stream
    processed_df = process_stream(kafka_df, schema)
    
    # Analyze trending videos
    top_channels_df, engagement_df = analyze_trending(processed_df, spark)
    
    # Start streaming queries
    queries = start_streaming_queries(top_channels_df, engagement_df)
    
    # Wait for queries to terminate
    for query in queries:
        query.awaitTermination()

if __name__ == "__main__":
    main()