# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: cleansing dan data quality
# MAGIC Flatten, parse timestamp, filter, deduplikasi. Setiap aturan dicatat ke tabel `dq_log`.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType

SCHEMA_NAME = "spotify"
spark.sql(f"USE {SCHEMA_NAME}")

MIN_MS_PLAYED = 30000

# COMMAND ----------

def flatten(df):
    """Flatten struct jadi kolom datar dan explode array.
    Export audio Spotify sudah datar, jadi fungsi ini no-op untuk data itu.
    Dipakai agar pipeline tetap aman jika ada field nested."""
    while True:
        nested = [f for f in df.schema.fields
                  if isinstance(f.dataType, (StructType, ArrayType))]
        if not nested:
            return df
        f = nested[0]
        if isinstance(f.dataType, StructType):
            keep = [c for c in df.columns if c != f.name]
            new = [F.col(f"{f.name}.{c.name}").alias(f"{f.name}_{c.name}")
                   for c in f.dataType.fields]
            df = df.select(*keep, *new)
        else:
            df = df.withColumn(f.name, F.explode_outer(f.name))

# COMMAND ----------

dq = []   # (check, rows_before, rows_after)

def log_step(name, before_df, after_df):
    b, a = before_df.count(), after_df.count()
    dq.append((name, b, a, b - a))
    print(f"{name}: {b} -> {a} (dibuang {b - a})")
    return after_df

# COMMAND ----------

bronze = flatten(spark.table("bronze_spotify_streams"))
print("Baris bronze:", bronze.count())

# 1. Rename dan parse timestamp
df = (bronze
      .withColumnRenamed("master_metadata_track_name", "track_name")
      .withColumnRenamed("master_metadata_album_artist_name", "artist_name")
      .withColumnRenamed("master_metadata_album_album_name", "album_name")
      .withColumnRenamed("spotify_track_uri", "track_uri")
      .withColumn("ts_utc", F.to_timestamp("ts")))

# 2. Timestamp valid
df1 = log_step("ts valid (tidak NULL)", df, df.filter(F.col("ts_utc").isNotNull()))

# 3. Buang micro-play
df2 = log_step(f"ms_played >= {MIN_MS_PLAYED}", df1,
               df1.filter(F.col("ms_played") >= MIN_MS_PLAYED))

# 4. Buang non-musik atau metadata kosong (podcast, NULL, string kosong)
has_meta = (F.col("track_uri").isNotNull()
            & (F.trim(F.col("track_name")) != "")
            & F.col("track_name").isNotNull()
            & F.col("artist_name").isNotNull()
            & (F.trim(F.col("artist_name")) != ""))
df3 = log_step("track dan artist tidak NULL/kosong", df2, df2.filter(has_meta))

# 5. Deduplikasi (timestamp + track)
df4 = log_step("dedup (ts_utc, track_uri)", df3,
               df3.dropDuplicates(["ts_utc", "track_uri"]))

# COMMAND ----------

# 6. Atribut waktu dan nilai default
silver = (df4
          .withColumn("stream_date", F.to_date("ts_utc"))
          .withColumn("year", F.year("ts_utc"))
          .withColumn("month", F.month("ts_utc"))
          .withColumn("day", F.dayofmonth("ts_utc"))
          .withColumn("hour", F.hour("ts_utc"))          # UTC
          .withColumn("album_name", F.coalesce("album_name", F.lit("Unknown Album")))
          .withColumn("shuffle", F.coalesce("shuffle", F.lit(False)))
          .withColumn("skipped", F.coalesce("skipped", F.lit(False)))
          .select("ts_utc", "stream_date", "year", "month", "day", "hour",
                  "ms_played", "track_uri", "track_name", "artist_name", "album_name",
                  "shuffle", "skipped", "platform", "reason_start", "reason_end",
                  "source_filename", "ingestion_timestamp"))

(silver.write.format("delta").mode("overwrite")
 .option("overwriteSchema", "true").saveAsTable("silver_spotify_streams"))

dq_df = spark.createDataFrame(dq, ["check_name", "rows_before", "rows_after", "rows_dropped"]) \
             .withColumn("run_ts", F.current_timestamp())
dq_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dq_log")

display(dq_df)

# COMMAND ----------

display(spark.table("silver_spotify_streams").limit(10))
