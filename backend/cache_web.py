import os
from supabase import create_client
import time
import random
import logging
from datetime import datetime

# Read from environment variables (set in Render dashboard)
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

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
    """
    Retrieve cached YouTube information for a Spotify URI from Supabase.
    
    Returns:
        tuple: (youtube_link, duration_seconds, title, match_pass) or (None, None, None, None) if not found.
    """
    try:
        res = _supabase.table("links").select("youtube_link, duration_seconds, title, match_pass")\
                    .eq("spotify_uri", spotify_uri).limit(1).execute()
        if res.data:
            return (res.data[0]["youtube_link"], 
                    res.data[0].get("duration_seconds"),
                    res.data[0].get("title"),
                    res.data[0].get("match_pass", 0))
    except Exception as e:
        logging.error(f"Cache read error: {e}")
    return None, None, None, None

@_retry_on_connection
def batch_insert_links(rows):
    """rows: list of dicts with song,artist,spotify_uri,youtube_link,username,duration_seconds,fetched_at,Album Name,title,match_pass"""
    if not rows:
        return
    _supabase.table("links").upsert(rows, on_conflict="spotify_uri").execute()

def prepare_link_row(song, artist, spotify_uri, link, username, duration_seconds, album=None, title=None, match_pass=0):
    """
    Create a row dict for the 'links' table.
    album is optional; if None, an empty string is used to avoid NOT NULL violation.
    title is optional (may be None for backward compatibility).
    match_pass is optional (default 0).
    """
    row = {
        "song": song,
        "artist": artist,
        "spotify_uri": spotify_uri,
        "youtube_link": link,
        "username": username,
        "fetched_at": datetime.utcnow().isoformat(),
        "duration_seconds": duration_seconds,
        "Album Name": album if album is not None else "",
        "match_pass": match_pass
    }
    if title is not None:
        row["title"] = title
    return row