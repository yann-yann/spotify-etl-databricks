# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Sample data generator (opsional)
# MAGIC Pakai notebook ini hanya kalau Extended Streaming History kamu belum datang.
# MAGIC Datanya sintetis, strukturnya sama dengan export Spotify.
# MAGIC Sengaja ada baris pendek, NULL, dan duplikat agar data quality check terlihat bekerja.

# COMMAND ----------

import json, random, hashlib
from datetime import datetime, timedelta

# Ubah sesuai workspace kamu.
# Free Edition (Unity Catalog): /Volumes/workspace/default/spotify_raw/
# Community Edition lama (DBFS): /dbfs/FileStore/spotify_raw/
VOLUME_SQL = "CREATE VOLUME IF NOT EXISTS workspace.default.spotify_raw"
RAW_DIR = "/Volumes/workspace/default/spotify_raw"

try:
    spark.sql(VOLUME_SQL)
except Exception as e:
    print("Lewati pembuatan volume:", e)

# COMMAND ----------

random.seed(42)

artists = ["Tulus", "Nadin Amizah", "Hindia", "Sheila On 7", "Coldplay", "Radiohead",
           "Daft Punk", "Kendrick Lamar", "Raisa", "Dewa 19", "Ed Sheeran", "NIKI"]
tracks = []
for i in range(60):
    artist = random.choice(artists)
    uri_id = hashlib.md5(f"track{i}".encode()).hexdigest()[:22]
    tracks.append({
        "uri": f"spotify:track:{uri_id}",
        "name": f"Song {i}",
        "artist": artist,
        "album": f"Album {i // 5}",
    })

start = datetime(2025, 6, 1)
end = datetime(2026, 10, 31)
span = int((end - start).total_seconds())

rows = []
for _ in range(25000):
    t = random.choice(tracks)
    ts = start + timedelta(seconds=random.randint(0, span))
    r = random.random()
    ms = random.randint(1000, 29000) if r < 0.12 else random.randint(30000, 280000)
    row = {
        "ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "platform": random.choice(["android", "windows", "ios"]),
        "ms_played": ms,
        "conn_country": "ID",
        "master_metadata_track_name": t["name"],
        "master_metadata_album_artist_name": t["artist"],
        "master_metadata_album_album_name": t["album"],
        "spotify_track_uri": t["uri"],
        "episode_name": None,
        "episode_show_name": None,
        "spotify_episode_uri": None,
        "reason_start": random.choice(["trackdone", "clickrow", "fwdbtn"]),
        "reason_end": random.choice(["trackdone", "fwdbtn", "endplay"]),
        "shuffle": random.random() < 0.4,
        "skipped": random.random() < 0.25,
        "offline": False,
        "incognito_mode": False,
    }
    rows.append(row)

# Podcast / baris tanpa metadata track (NULL)
for row in random.sample(rows, 400):
    row["master_metadata_track_name"] = None
    row["master_metadata_album_artist_name"] = None
    row["spotify_track_uri"] = None
    row["episode_name"] = "Episode X"

# Duplikat
rows += random.sample(rows, 300)

# Beberapa hari dengan lagu yang diputar >10 kali
hot = tracks[0]
for d in range(5):
    day = datetime(2026, 9, 10 + d)
    for k in range(14):
        rows.append({**rows[0],
                     "ts": (day + timedelta(minutes=20 * k)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                     "ms_played": 180000,
                     "master_metadata_track_name": hot["name"],
                     "master_metadata_album_artist_name": hot["artist"],
                     "master_metadata_album_album_name": hot["album"],
                     "spotify_track_uri": hot["uri"],
                     "skipped": False})

path = f"{RAW_DIR}/Streaming_History_Audio_sample.json"
with open(path, "w") as f:
    json.dump(rows, f)
print("Tulis", len(rows), "baris ke", path)
