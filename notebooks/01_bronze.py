# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: raw ingestion
# MAGIC Baca JSON mentah, tambah metadata, simpan ke Delta apa adanya.
# MAGIC Tidak ada pembersihan di layer ini.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, LongType, BooleanType)

RAW_PATH = "/Volumes/workspace/default/spotify_raw/"   # ubah jika perlu
SCHEMA_NAME = "spotify"
BRONZE_TABLE = "bronze_spotify_streams"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
spark.sql(f"USE {SCHEMA_NAME}")

# COMMAND ----------

# Schema eksplisit.
# Kolom IP dan user agent sengaja tidak dibaca karena data pribadi dan tidak dibutuhkan.
raw_schema = StructType([
    StructField("ts", StringType()),
    StructField("platform", StringType()),
    StructField("ms_played", LongType()),
    StructField("conn_country", StringType()),
    StructField("master_metadata_track_name", StringType()),
    StructField("master_metadata_album_artist_name", StringType()),
    StructField("master_metadata_album_album_name", StringType()),
    StructField("spotify_track_uri", StringType()),
    StructField("episode_name", StringType()),
    StructField("episode_show_name", StringType()),
    StructField("spotify_episode_uri", StringType()),
    StructField("reason_start", StringType()),
    StructField("reason_end", StringType()),
    StructField("shuffle", BooleanType()),
    StructField("skipped", BooleanType()),
    StructField("offline", BooleanType()),
    StructField("incognito_mode", BooleanType()),
])

# COMMAND ----------

raw = (spark.read
       .schema(raw_schema)
       .option("multiLine", True)              # export Spotify berupa JSON array
       .option("pathGlobFilter", "Streaming_History_Audio_*.json")  # cocok untuk data asli dan sample
       .json(RAW_PATH))

bronze = (raw
          .withColumn("ingestion_timestamp", F.current_timestamp())
          .withColumn("source_filename", F.col("_metadata.file_name")))

(bronze.write
 .format("delta")
 .mode("append")          # Bronze append-only. Duplikat akibat rerun ditangani di Silver.
 .saveAsTable(BRONZE_TABLE))

print("Baris di bronze:", spark.table(BRONZE_TABLE).count())

# COMMAND ----------

display(spark.table(BRONZE_TABLE).limit(10))
