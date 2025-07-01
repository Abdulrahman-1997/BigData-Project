from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.mongodb.output.uri", "mongodb://mongodb:27017/bigdata.events") \
    .config("spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1," \
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "user_events") \
    .load()

parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json("json_str", """
        event_id STRING,
        user_id INT,
        event_type STRING,
        timestamp STRING,
        device STRING
    """).alias("data")) \
    .select("data.*")

def write_to_mongo(batch_df, batch_id):
    count = batch_df.count()
    print(f"[INFO] Batch {batch_id} has {count} rows")
    try:
        batch_df.write \
            .format("mongo") \
            .mode("append") \
            .option("uri", "mongodb://mongodb:27017") \
            .option("database", "bigdata") \
            .option("collection", "events") \
            .save()
        print(f"[INFO] Batch {batch_id} written to MongoDB successfully")
    except Exception as e:
        print(f"[ERROR] Failed to write batch {batch_id} to MongoDB: {e}")

query1 = parsed_df.writeStream \
    .format("parquet") \
    .option("path", "/app/parquet_data") \
    .option("checkpointLocation", "/app/checkpoint") \
    .outputMode("append") \
    .start()

query2 = parsed_df.writeStream \
    .foreachBatch(write_to_mongo) \
    .outputMode("append") \
    .start()

query1.awaitTermination()
query2.awaitTermination()
