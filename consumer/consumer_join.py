from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, window
from pyspark.sql.types import StructType, StringType, IntegerType

spark = SparkSession.builder \
    .appName("Gudang Monitoring") \
    .getOrCreate()

# Schema
schema_suhu = StructType() \
    .add("gudang_id", StringType()) \
    .add("suhu", IntegerType())

schema_kelembaban = StructType() \
    .add("gudang_id", StringType()) \
    .add("kelembaban", IntegerType())

# Stream suhu
suhu_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .load()

suhu_df = suhu_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_suhu).alias("data")) \
    .select("data.*")

# Stream kelembaban
kelembaban_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .load()

kelembaban_df = kelembaban_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema_kelembaban).alias("data")) \
    .select("data.*")

# Filter & Join
peringatan_df = suhu_df.join(kelembaban_df, "gudang_id") \
    .withColumn("status", expr("""
        CASE
            WHEN suhu > 80 AND kelembaban > 70 THEN 'Bahaya tinggi! Barang berisiko rusak'
            WHEN suhu > 80 THEN 'Suhu tinggi, kelembaban normal'
            WHEN kelembaban > 70 THEN 'Kelembaban tinggi, suhu aman'
            ELSE 'Aman'
        END
    """))

query = peringatan_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()