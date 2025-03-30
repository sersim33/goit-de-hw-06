
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql import SparkSession
from configs import kafka_config
import os

# Налаштування середовища для роботи з Kafka
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# Створення SparkSession
spark = (SparkSession.builder
         .appName("KafkaStreaming")
         .master("local[*]")
         .getOrCreate())

# Зчитування потоку з Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("subscribe", "Sergii_S_building_sensors") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "5") \
    .load()

# Визначення схеми JSON-даних
json_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True)
])

# Обробка даних
clean_df = df.selectExpr("CAST(value AS STRING) AS value_deserialized") \
    .withColumn("value_json", from_json(col("value_deserialized"), json_schema)) \
    .withColumn("timestamp", from_unixtime(col("value_json.timestamp").cast(DoubleType())).cast("timestamp")) \
    .withColumn("temperature", col("value_json.temperature")) \
    .withColumn("humidity", col("value_json.humidity")) \
    .drop("value_json", "value_deserialized")

# Визначення віконних середніх значень
agg_df = clean_df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(
        window("timestamp", "1 minute", "30 seconds")
    ) \
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity")
    )

# Зчитування умов алертів
alerts_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("humidity_min", DoubleType(), True),
    StructField("humidity_max", DoubleType(), True),
    StructField("temperature_min", DoubleType(), True),
    StructField("temperature_max", DoubleType(), True),
    StructField("code", IntegerType(), True),
    StructField("message", StringType(), True)
])

alerts_df = spark.read.csv("alerts_conditions.csv", schema=alerts_schema, header=True)

# Виконання cross join та фільтрація алертів
alerts_joined_df = agg_df.crossJoin(alerts_df).filter(
    (col("avg_temperature") >= col("temperature_min")) & (col("avg_temperature") <= col("temperature_max")) |
    (col("avg_humidity") >= col("humidity_min")) & (col("avg_humidity") <= col("humidity_max"))
).select("window", "avg_temperature", "avg_humidity", "code", "message")

# Виведення алертів у консоль
alerts_query = alerts_joined_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", "/tmp/checkpoints-alerts") \
    .start()

# Запис алертів у Kafka
to_kafka_df = alerts_joined_df.select(
    to_json(struct("window", "avg_temperature", "avg_humidity", "code", "message")).alias("value")
)

kafka_query = to_kafka_df.writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("topic", "Sergii_S_spark_alerts") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';") \
    .option("checkpointLocation", "/tmp/checkpoints-kafka") \
    .start()

alerts_query.awaitTermination()
kafka_query.awaitTermination()










