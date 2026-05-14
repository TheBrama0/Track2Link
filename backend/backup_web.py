import pandas as pd
from supabase import create_client
import time, logging
from cache_web import _retry_on_connection, _supabase  # reuse same client

COLUMN_MAPPING = {
    "Track URI": "spotify_uri",
    "Track Name": "track_name",
    "Artist Name(s)": "artist_name",
    "Album Name": "album_name",
    "Album Release Date": "album_release_date",
    "Album Image URL": "album_image_url",
    "Disc Number": "disc_number",
    "Track Number": "track_number",
    "Track Duration (ms)": "track_duration_ms",
    "Duration (ms)": "track_duration_ms",
    "Track Preview URL": "track_preview_url",
    "Explicit": "explicit",
    "Popularity": "popularity",
    "ISRC": "isrc",
    "Added By": "added_by",
    "Added At": "added_at",
    "Release Date": "release_date",
    "Genres": "genres",
    "Record Label": "record_label",
    "Danceability": "danceability",
    "Energy": "energy",
    "Key": "key",
    "Loudness": "loudness",
    "Mode": "mode",
    "Speechiness": "speechiness",
    "Acousticness": "acousticness",
    "Instrumentalness": "instrumentalness",
    "Liveness": "liveness",
    "Valence": "valence",
    "Tempo": "tempo",
    "Time Signature": "time_signature",
}

@_retry_on_connection
def batch_upsert_tracks(rows):
    """rows: list of dicts with column names matching the 'tracks' table"""
    if not rows:
        return
    _supabase.table("tracks").upsert(rows, on_conflict="spotify_uri").execute()

def build_track_data_from_csv(df):
    """Convert a pandas DataFrame into a list of dicts ready for upsert.
       Only includes columns that exist in the CSV."""
    available = {}
    for csv_col, db_col in COLUMN_MAPPING.items():
        if csv_col in df.columns:
            available[csv_col] = db_col
        elif csv_col == "Track Duration (ms)" and "Duration (ms)" in df.columns:
            available[csv_col] = db_col

    rows = []
    for _, row in df.iterrows():
        uri = str(row.get("Track URI", "")).strip()
        if not uri or uri.lower() == "nan":
            continue
        data = {"spotify_uri": uri}
        for csv_col, db_col in available.items():
            if csv_col == "Track URI":
                continue
            val = row.get(csv_col)
            if pd.isna(val):
                data[db_col] = None
            else:
                if db_col == "explicit":
                    data[db_col] = str(val).upper() == "TRUE"
                elif db_col in ("popularity", "key", "mode", "time_signature",
                                "disc_number", "track_number", "track_duration_ms"):
                    try: data[db_col] = int(float(val))
                    except: data[db_col] = None
                elif db_col in ("danceability", "energy", "loudness", "speechiness",
                                "acousticness", "instrumentalness", "liveness",
                                "valence", "tempo"):
                    try: data[db_col] = float(val)
                    except: data[db_col] = None
                else:
                    data[db_col] = str(val)
        rows.append(data)
    return rows