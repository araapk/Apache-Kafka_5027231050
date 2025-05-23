# spark_consumer/pyspark_consumer_processor.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, when
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# --- KONFIGURASI PENTING ---
# Sesuaikan KAFKA_PACKAGE dengan versi Spark dan Scala yang Anda gunakan.
# Contoh: Jika Spark Anda 3.5.5 dan menggunakan Scala 2.12 (seperti yang kita identifikasi):
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"
# Jika versi 3.5.5 tidak ada, coba 3.5.0:
# KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

KAFKA_BROKER = "localhost:9092"
TOPIC_SUHU = "sensor-suhu-gudang"
TOPIC_KELEMBABAN = "sensor-kelembaban-gudang"
# --- AKHIR KONFIGURASI PENTING ---

# Fungsi untuk memproses dan mencetak batch data individual (Tugas 3)
def print_individual_alert_batch(df, epoch_id, alert_type_header, value_column_name, unit):
    if not df.isEmpty(): # Lebih efisien daripada df.count() > 0 untuk sekedar cek kosong
        alerts = df.collect()
        for alert in alerts:
            print(f"\n{alert_type_header}")
            print(f"Gudang {alert['gudang_id']}: {value_column_name} {alert[value_column_name]:.1f}{unit}") # Format ke 1 desimal

# Fungsi untuk memproses dan mencetak batch data gabungan (Tugas 4)
def print_combined_status_batch(df, epoch_id):
    if not df.isEmpty():
        statuses = df.collect()
        for status in statuses:
            if status['is_critical']:
                print("\n[PERINGATAN KRITIS]")
            else:
                print("\n[STATUS GUDANG TERKINI]")
            
            print(f"Gudang: {status['gudang_id']}")
            print(f"  - Suhu: {status['suhu']:.1f}°C")
            print(f"  - Kelembaban: {status['kelembaban']:.1f}%")
            print(f"  - Status: {status['status_pesan']}")

def main():
    spark = (SparkSession.builder
             .appName("WarehouseMonitor")
             .config("spark.jars.packages", KAFKA_PACKAGE)
             .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
             .config("spark.sql.shuffle.partitions", "2") # Mengurangi jumlah partisi shuffle, bisa membantu di local mode
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    suhu_schema = StructType([
        StructField("gudang_id", StringType(), True),
        StructField("suhu", FloatType(), True)
    ])

    kelembaban_schema = StructType([
        StructField("gudang_id", StringType(), True),
        StructField("kelembaban", FloatType(), True)
    ])

    suhu_df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_SUHU) \
        .option("startingOffsets", "latest") \
        .load()

    kelembaban_df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_KELEMBABAN) \
        .option("startingOffsets", "latest") \
        .load()

    suhu_df = suhu_df_raw.select(
        from_json(col("value").cast("string"), suhu_schema).alias("data"),
        col("timestamp").alias("suhu_timestamp")
    ).select("data.*", "suhu_timestamp")

    kelembaban_df = kelembaban_df_raw.select(
        from_json(col("value").cast("string"), kelembaban_schema).alias("data"),
        col("timestamp").alias("kelembaban_timestamp")
    ).select("data.*", "kelembaban_timestamp")

    # --- TUGAS 3: Filtering Individual ---
    suhu_alerts_df = suhu_df.filter(col("suhu") > 80.0)
    suhu_alerts_query = suhu_alerts_df.writeStream \
        .outputMode("append") \
        .trigger(processingTime="15 seconds") \
        .foreachBatch(lambda df, epoch_id: print_individual_alert_batch(df, epoch_id, "[Peringatan Suhu Tinggi]", "suhu", "°C")) \
        .start()

    kelembaban_alerts_df = kelembaban_df.filter(col("kelembaban") > 70.0)
    kelembaban_alerts_query = kelembaban_alerts_df.writeStream \
        .outputMode("append") \
        .trigger(processingTime="15 seconds") \
        .foreachBatch(lambda df, epoch_id: print_individual_alert_batch(df, epoch_id, "[Peringatan Kelembaban Tinggi]", "kelembaban", "%")) \
        .start()

    # --- TUGAS 4: Gabungkan Stream ---
    suhu_df_watermarked = suhu_df.withWatermark("suhu_timestamp", "20 seconds")
    kelembaban_df_watermarked = kelembaban_df.withWatermark("kelembaban_timestamp", "20 seconds")
    
    s_df = suhu_df_watermarked.alias("s")
    k_df = kelembaban_df_watermarked.alias("k")

    # PERBAIKAN: Menggunakan 'how' bukan 'joinType'
    joined_df = s_df.join(
        k_df,
        expr("""
            s.gudang_id = k.gudang_id AND 
            s.suhu_timestamp >= k.kelembaban_timestamp - interval 10 seconds AND
            s.suhu_timestamp <= k.kelembaban_timestamp + interval 10 seconds 
        """),
        how="inner" # Parameter yang benar adalah 'how'
    ).select(
        col("s.gudang_id").alias("gudang_id"),
        col("s.suhu").alias("suhu"),
        col("k.kelembaban").alias("kelembaban")
    )
    
    critical_condition_df = joined_df.withColumn(
        "is_critical",
        (col("suhu") > 80.0) & (col("kelembaban") > 70.0)
    ).withColumn(
        "status_pesan",
        when((col("suhu") > 80.0) & (col("kelembaban") > 70.0), "Bahaya tinggi! Barang berisiko rusak")
        .when(col("suhu") > 80.0, "Suhu tinggi, kelembaban normal")
        .when(col("kelembaban") > 70.0, "Kelembaban tinggi, suhu normal")
        .otherwise("Aman")
    )

    combined_status_query = critical_condition_df.writeStream \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .foreachBatch(print_combined_status_batch) \
        .start()

    # Menunggu query utama selesai. Anda bisa memilih untuk menghentikan query lain jika ini selesai.
    # Atau gunakan spark.streams.awaitAnyTermination() jika Anda ingin aplikasi berhenti jika salah satu query gagal/selesai.
    try:
        combined_status_query.awaitTermination()
    except KeyboardInterrupt:
        print("Aplikasi streaming dihentikan oleh pengguna.")
    finally:
        print("Menghentikan query streaming lainnya...")
        # Opsional: Hentikan query lain secara eksplisit jika belum berhenti
        if suhu_alerts_query.isActive:
            suhu_alerts_query.stop()
        if kelembaban_alerts_query.isActive:
            kelembaban_alerts_query.stop()
        print("Semua query streaming telah dihentikan atau selesai.")
        spark.stop() # Hentikan SparkSession
        print("SparkSession dihentikan.")


if __name__ == "__main__":
    main()