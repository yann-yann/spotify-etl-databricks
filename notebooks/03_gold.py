# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: star schema
# MAGIC `fact_streams`, `dim_track`, `dim_date`. Lalu OPTIMIZE dan Z-ORDER.

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql("USE spotify")
silver = spark.table("silver_spotify_streams")

# COMMAND ----------

# dim_track
track_id = F.element_at(F.split("track_uri", ":"), -1)

dim_track = (silver
             .withColumn("track_id", track_id)
             .groupBy("track_id")
             .agg(F.max("track_name").alias("track_name"),
                  F.max("artist_name").alias("artist_name"),
                  F.max("album_name").alias("album_name")))

dim_track.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable("dim_track")

# COMMAND ----------

# dim_date: satu baris per hari dari tanggal pertama sampai terakhir
b = silver.agg(F.min("stream_date").alias("mn"), F.max("stream_date").alias("mx")).first()

dim_date = (spark.range(1)
            .select(F.explode(F.sequence(F.lit(b["mn"]), F.lit(b["mx"]),
                                         F.expr("interval 1 day"))).alias("date"))
            .select(F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
                    "date",
                    F.date_format("date", "EEEE").alias("day_of_week"),
                    F.dayofweek("date").isin(1, 7).alias("is_weekend"),
                    F.month("date").alias("month"),
                    F.year("date").alias("year")))

dim_date.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable("dim_date")

# COMMAND ----------

# fact_streams
fact = (silver
        .select(
            F.sha2(F.concat_ws("|", F.col("ts_utc").cast("string"), F.col("track_uri")), 256)
             .alias("stream_id"),
            track_id.alias("track_id"),
            F.date_format("stream_date", "yyyyMMdd").cast("int").alias("date_key"),
            F.col("ts_utc").alias("stream_ts"),
            F.col("hour"),
            F.col("ms_played"),
            (F.col("ms_played") / 1000.0).alias("seconds_played"),
            F.col("shuffle").alias("shuffle_mode"),
            F.col("skipped"),
            F.col("platform")))

fact.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable("fact_streams")

# COMMAND ----------

# Optimasi Delta. Jika Free tier menolak perintah ini, catat di README.
try:
    spark.sql("OPTIMIZE fact_streams ZORDER BY (date_key)")
    print("OPTIMIZE + ZORDER selesai")
except Exception as e:
    print("OPTIMIZE gagal:", e)

# COMMAND ----------

for t in ["fact_streams", "dim_track", "dim_date"]:
    print(t, spark.table(t).count())
