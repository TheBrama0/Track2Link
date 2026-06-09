import sqlite3
import json
import logging
from typing import Optional, Tuple, Dict, Any, List

TRACKS_DB = "tracks_cache.db"
LINKS_DB = "links_cache.db"


def _get_conn(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_dbs():
    """Create tables if they don't exist."""
    # Tracks cache
    with _get_conn(TRACKS_DB) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS tracks_cache (
                spotify_uri TEXT PRIMARY KEY,
                full_row_json TEXT,
                synced INTEGER DEFAULT 0
            )
        """)
    # Links cache – add match_pass column
    with _get_conn(LINKS_DB) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS links_cache (
                spotify_uri TEXT PRIMARY KEY,
                youtube_link TEXT,
                duration_seconds REAL,
                title TEXT,
                full_row_json TEXT,
                synced INTEGER DEFAULT 0,
                match_pass INTEGER DEFAULT 0
            )
        """)
        # For existing databases, add column if missing
        try:
            conn.execute("ALTER TABLE links_cache ADD COLUMN match_pass INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass  # column already exists
        conn.execute("CREATE INDEX IF NOT EXISTS idx_links_uri ON links_cache(spotify_uri)")


# ---------- tracks operations ----------
def is_track_seen(spotify_uri: str) -> bool:
    with _get_conn(TRACKS_DB) as conn:
        row = conn.execute("SELECT 1 FROM tracks_cache WHERE spotify_uri = ?", (spotify_uri,)).fetchone()
        return row is not None


def add_pending_track(spotify_uri: str, row_dict: Dict[str, Any]):
    with _get_conn(TRACKS_DB) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO tracks_cache (spotify_uri, full_row_json, synced) VALUES (?, ?, 0)",
            (spotify_uri, json.dumps(row_dict))
        )


def seed_tracks_from_supabase(supabase_client):
    """One‑time: pull all existing spotify_uri from Supabase tracks into local cache (synced=1)."""
    offset = 0
    limit = 1000
    with _get_conn(TRACKS_DB) as conn:
        while True:
            res = supabase_client.table("tracks").select("spotify_uri").range(offset, offset + limit - 1).execute()
            if not res.data:
                break
            for row in res.data:
                conn.execute(
                    "INSERT OR IGNORE INTO tracks_cache (spotify_uri, synced) VALUES (?, 1)",
                    (row["spotify_uri"],)
                )
            offset += limit
            if len(res.data) < limit:
                break
        conn.commit()


# ---------- links operations ----------
def get_cached_link(spotify_uri: str) -> Tuple[Optional[str], Optional[float], Optional[str], Optional[int]]:
    with _get_conn(LINKS_DB) as conn:
        row = conn.execute(
            "SELECT youtube_link, duration_seconds, title, match_pass FROM links_cache WHERE spotify_uri = ?",
            (spotify_uri,)
        ).fetchone()
        if row:
            return (row["youtube_link"], 
                    row["duration_seconds"], 
                    row["title"], 
                    row.get("match_pass", 0))
    return None, None, None, None


def add_pending_link(spotify_uri: str, youtube_link: str, duration_seconds: float, title: str,
                     full_row_dict: Dict[str, Any], match_pass: int = 0):
    with _get_conn(LINKS_DB) as conn:
        conn.execute(
            """INSERT OR REPLACE INTO links_cache 
               (spotify_uri, youtube_link, duration_seconds, title, full_row_json, synced, match_pass) 
               VALUES (?, ?, ?, ?, ?, 0, ?)""",
            (spotify_uri, youtube_link, duration_seconds, title, json.dumps(full_row_dict), match_pass)
        )


def seed_links_from_supabase(supabase_client):
    """One‑time: pull all existing links from Supabase and store permanent columns + full JSON (synced=1)."""
    offset = 0
    limit = 1000
    with _get_conn(LINKS_DB) as conn:
        while True:
            res = supabase_client.table("links").select("*").range(offset, offset + limit - 1).execute()
            if not res.data:
                break
            for row in res.data:
                conn.execute(
                    """INSERT OR REPLACE INTO links_cache 
                       (spotify_uri, youtube_link, duration_seconds, title, full_row_json, synced, match_pass) 
                       VALUES (?, ?, ?, ?, ?, 1, ?)""",
                    (row["spotify_uri"], 
                     row.get("youtube_link"), 
                     row.get("duration_seconds"),
                     row.get("title"), 
                     json.dumps(row),
                     row.get("match_pass", 0))
                )
            offset += limit
            if len(res.data) < limit:
                break
        conn.commit()


# ---------- sync helpers (for background worker) ----------
def get_unsynced_tracks(limit: int = 100) -> List[Tuple[str, str]]:
    """
    Return list of (spotify_uri, full_row_json) for tracks with synced=0.
    """
    with _get_conn(TRACKS_DB) as conn:
        rows = conn.execute(
            "SELECT spotify_uri, full_row_json FROM tracks_cache WHERE synced = 0 LIMIT ?",
            (limit,)
        ).fetchall()
        return [(row["spotify_uri"], row["full_row_json"]) for row in rows]


def get_unsynced_links(limit: int = 100) -> List[sqlite3.Row]:
    """
    Return list of rows (as sqlite3.Row) for links with synced=0.
    Each row contains: spotify_uri, youtube_link, duration_seconds, title, full_row_json, match_pass.
    """
    with _get_conn(LINKS_DB) as conn:
        rows = conn.execute(
            "SELECT spotify_uri, youtube_link, duration_seconds, title, full_row_json, match_pass "
            "FROM links_cache WHERE synced = 0 LIMIT ?",
            (limit,)
        ).fetchall()
        return rows


def mark_synced(uris: List[str], table: str):
    """
    Set synced=1 for the given list of spotify_uris.
    table must be either 'tracks' or 'links'.
    """
    if not uris:
        return
    db_path = TRACKS_DB if table == "tracks" else LINKS_DB
    with _get_conn(db_path) as conn:
        placeholders = ','.join('?' for _ in uris)
        conn.execute(f"UPDATE {table}_cache SET synced = 1 WHERE spotify_uri IN ({placeholders})", uris)
