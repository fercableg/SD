import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define the schema for the incoming JSON messages from Kafka
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("query_type", StringType(), True),
    StructField("latency_ms", DoubleType(), True),
    StructField("cache_hit", BooleanType(), True),
    StructField("retry_count", IntegerType(), True),  # Note: can be null
    StructField("status", StringType(), True),
    StructField("zone_id", StringType(), True)
])

def write_batch_to_es(batch_df, batch_id):
    if not batch_df.isEmpty():
        # Asegurar que las columnas tengan nombres compatibles con ES (sin caracteres especiales)
        es_df = batch_df.select(
            col("window_start").alias("window_start"),
            col("window_end").alias("window_end"),
            col("total_attempts"),
            col("hits"),
            col("hit_rate"),
            col("latency_p50"),
            col("latency_p95"),
            col("retry_rate")
        )
        es_df.write \
            .format("org.elasticsearch.spark.sql") \
            .mode("append") \
            .option("es.resource", "metrics-aggregated") \
            .option("es.nodes", "elasticsearch") \
            .option("es.port", "9200") \
            .option("es.nodes.wan.only", "true") \
            .option("es.write.operation", "index") \
            .save()

def main():
    spark = SparkSession.builder \
        .appName("MetricsStreaming") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    # Reduce log verbosity
    spark.sparkContext.setLogLevel("WARN")

    # Read from Kafka (topic: consultas-principal)
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "consultas-principal") \
        .option("startingOffsets", "latest") \
        .load()

    # Parse the JSON value
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Convert timestamp string to timestamp type
    # Nota: se usa parsed_df directamente y se asignan las nuevas columnas
    processed_df = parsed_df.withColumn(
        "event_time", to_timestamp(col("timestamp"))
    ).withColumn(
        "latency_ms", col("latency_ms").cast("double")
    ).withColumn(
        "retry_count", col("retry_count").cast("integer")
    )

    # Define window: 1 minute window, sliding every 30 seconds
    windowed_df = (
        processed_df
        .withWatermark("event_time", "1 minute")
        .groupBy(
            window(col("event_time"), "1 minute", "30 seconds")
        )
        .agg(
            count("*").alias("total_attempts"),
            sum(when(col("cache_hit") == True, 1).otherwise(0)).alias("hits"),
            approx_percentile("latency_ms", 0.5).alias("latency_p50"),
            approx_percentile("latency_ms", 0.95).alias("latency_p95"),
            sum(when(col("retry_count").isNotNull() & (col("retry_count") > 0), 1).otherwise(0)).alias("retries")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("total_attempts"),
            col("hits"),
            (col("hits") / col("total_attempts")).alias("hit_rate"),
            col("latency_p50"),
            col("latency_p95"),
            (col("retries") / col("total_attempts")).alias("retry_rate")
        )
    )

    # Write to Elasticsearch using foreachBatch
    query = windowed_df.writeStream \
        .foreachBatch(write_batch_to_es) \
        .outputMode("update") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .trigger(processingTime='30 seconds') \
        .start()

    # Wait for the query to terminate
    query.awaitTermination()

if __name__ == "__main__":
    main()