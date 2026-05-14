from supabase import create_client
import time
import random
import logging
from datetime import datetime

SUPABASE_URL = "URL_HERE"   # NEVER in frontend code
SUPABASE_SERVICE_KEY = "SERVICE_KEY_HERE"   # NEVER in frontend code

_supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

MAX_RETRIES = 3

def _retry_on_connection(func):
    def wrapper(*args, **kwargs):
        last_exc = None
        for attempt in range(MAX_RETRIES):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_exc = e
                if not _is_connection_error(e):
                    break
                delay = (2 ** attempt) + random.uniform(0, 0.5)
                logging.warning(f"Retrying in {delay:.1f}s: {e}")
                time.sleep(delay)
        raise last_exc
    return wrapper

def _is_connection_error(e):
    msg = str(e).lower()
    return any(k in msg for k in ["connection", "timeout", "cloudflare"])

def get_cached_link(spotify_uri):
    try:
        res = _supabase.table("links").select("youtube_link, duration_seconds")\
                    .eq("spotify_uri", spotify_uri).limit(1).execute()
        if res.data:
            return res.data[0]["youtube_link"], res.data[0].get("duration_seconds")
    except Exception as e:
        logging.error(f"Cache read error: {e}")
    return None, None

@_retry_on_connection
def batch_insert_links(rows):
    """rows: list of dicts with song,artist,spotify_uri,youtube_link,username,duration_seconds,fetched_at"""
    if not rows:
        return
    _supabase.table("links").upsert(rows, on_conflict="spotify_uri").execute()

def prepare_link_row(song, artist, spotify_uri, link, username, duration_seconds):
    return {
        "song": song,
        "artist": artist,
        "spotify_uri": spotify_uri,
        "youtube_link": link,
        "username": username,
        "fetched_at": datetime.utcnow().isoformat(),
        "duration_seconds": duration_seconds
    }