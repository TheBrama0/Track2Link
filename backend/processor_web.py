import pandas as pd
import time
import logging
import os
from tasks import get_task
from cache_web import get_cached_link, prepare_link_row, batch_insert_links, _supabase
from backup_web import batch_upsert_tracks, build_track_data_from_csv
from youtube import search_youtube

SONG_COL = "Track Name"
ARTIST_COL = "Artist Name(s)"
SPOTIFY_URI_COL = "Track URI"
YOUTUBE_LINK_COL = "YouTube Link"
YOUTUBE_DURATION_COL = "YouTube Duration"
POSSIBLE_DURATION_COLS = ["Duration (ms)", "Track Duration (ms)"]

def add_log(task_id, msg):
    task = get_task(task_id)
    if task:
        task['logs'].append(msg)

def get_existing_uris(uri_list, batch_size=100):
    """
    Query Supabase tracks table to find which spotify_uri already exist.
    Returns a set of existing URIs.
    """
    existing = set()
    for i in range(0, len(uri_list), batch_size):
        batch_uris = uri_list[i:i+batch_size]
        try:
            res = _supabase.table("tracks").select("spotify_uri").in_("spotify_uri", batch_uris).execute()
            if res.data:
                existing.update(row["spotify_uri"] for row in res.data)
        except Exception as e:
            logging.warning(f"Failed to check existing URIs: {e}")
            continue
    return existing

def process_csv(file_path, settings, username, task_id):
    """
    username = session_id (unique task ID) – used to track which session added each link.
    """
    # --- Read CSV with encoding fallback ---
    encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']
    df = None
    for enc in encodings:
        try:
            df = pd.read_csv(file_path, encoding=enc).dropna(how='all')
            add_log(task_id, f"Read CSV with {enc} encoding")
            break
        except UnicodeDecodeError:
            continue
    if df is None:
        add_log(task_id, "Error: Could not read CSV with any common encoding.")
        return

    # --- Prepare track data and filter only NEW tracks ---
    add_log(task_id, "Checking for new tracks to backup...")
    tracks_data = build_track_data_from_csv(df)
    all_uris = [row["spotify_uri"] for row in tracks_data if row.get("spotify_uri")]
    existing_uris = get_existing_uris(all_uris)
    new_tracks = [row for row in tracks_data if row["spotify_uri"] not in existing_uris]

    if new_tracks:
        add_log(task_id, f"Backing up {len(new_tracks)} new tracks (skipped {len(existing_uris)} existing).")
        batch_size = 50
        for i in range(0, len(new_tracks), batch_size):
            batch = new_tracks[i:i+batch_size]
            try:
                batch_upsert_tracks(batch)
                time.sleep(0.2)  # mild rate limiting
            except Exception as e:
                add_log(task_id, f"Backup error: {e}")
                logging.exception("Backup batch failed")
        add_log(task_id, "Track backup complete (new tracks only).")
    else:
        add_log(task_id, "No new tracks to back up (all already in database).")

    # --- Validate required columns for YouTube processing ---
    required_cols = [SONG_COL, ARTIST_COL, SPOTIFY_URI_COL]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        add_log(task_id, f"Error: Missing required columns: {missing}")
        return

    # Duration column (optional)
    duration_col = None
    for col in POSSIBLE_DURATION_COLS:
        if col in df.columns:
            duration_col = col
            break

    # Add output columns if missing
    if YOUTUBE_LINK_COL not in df.columns:
        df[YOUTUBE_LINK_COL] = ''
    if YOUTUBE_DURATION_COL not in df.columns:
        df[YOUTUBE_DURATION_COL] = None

    total_rows = len(df)
    processed = 0
    new_links = 0
    limit_type = settings['limit_type']
    limit_value = settings['limit_value']

    link_batch = []
    temp_file = file_path + ".tmp"

    for idx, row in df.iterrows():
        # --- Limit checks (only 'rows' mode now) ---
        if limit_type == 'rows' and limit_value > 0 and processed >= limit_value:
            add_log(task_id, f"Reached row limit ({limit_value}). Stopping.")
            break

        # Skip if already has a YouTube link
        existing_link = row.get(YOUTUBE_LINK_COL)
        if pd.notna(existing_link) and str(existing_link).startswith('http'):
            processed += 1
            continue

        # Extract song, artist, URI
        song = str(row.get(SONG_COL, '')).strip()
        artist = str(row.get(ARTIST_COL, '')).strip()
        spotify_uri = str(row.get(SPOTIFY_URI_COL, '')).strip()

        if not song or not artist or song.lower() == 'nan' or artist.lower() == 'nan':
            processed += 1
            continue
        if not spotify_uri or spotify_uri.lower() == 'nan':
            processed += 1
            continue

        # Target duration (in seconds) from CSV, if available
        target_sec = None
        if duration_col and pd.notna(row.get(duration_col)):
            try:
                target_sec = float(row[duration_col]) / 1000.0
            except Exception:
                pass

        # Check Supabase cache first
        link, cached_dur = get_cached_link(spotify_uri)
        if link:
            df.at[idx, YOUTUBE_LINK_COL] = link
            if cached_dur is not None:
                df.at[idx, YOUTUBE_DURATION_COL] = cached_dur
            new_links += 1
            add_log(task_id, f"Inserted (cache): {song}")
        else:
            # YouTube search
            try:
                link, api_dur = search_youtube(song, artist, settings['api_key'],
                                               target_duration_sec=target_sec)
            except Exception as e:
                add_log(task_id, f"Error searching {song}: {e}")
                link = None
                api_dur = None

            if link:
                df.at[idx, YOUTUBE_LINK_COL] = link
                if api_dur is not None:
                    df.at[idx, YOUTUBE_DURATION_COL] = api_dur
                new_links += 1

                # username here is the session_id (task_id)
                link_batch.append(prepare_link_row(song, artist, spotify_uri,
                                                   link, username, api_dur))
                add_log(task_id, f"Inserted: {song}")

                # Flush batch every 10 new links
                if len(link_batch) >= 10:
                    try:
                        batch_insert_links(link_batch)
                        add_log(task_id, f"Flushed {len(link_batch)} links to database")
                    except Exception as e:
                        add_log(task_id, f"Link cache error (batch flush): {e}")
                    link_batch = []

                # Delay between YouTube API calls
                time.sleep(settings['delay'])

        processed += 1

        # Periodic save (every 10 rows)
        if processed % 10 == 0:
            try:
                df.to_csv(temp_file, index=False, encoding='utf-8-sig')
                os.replace(temp_file, file_path)
            except Exception as e:
                add_log(task_id, f"Warning: Could not save CSV: {e}")

        # Update progress
        task = get_task(task_id)
        if task:
            task['progress'] = int((processed / total_rows) * 100)

    # Flush any remaining link inserts
    if link_batch:
        try:
            batch_insert_links(link_batch)
            add_log(task_id, f"Final flush: {len(link_batch)} links saved")
        except Exception as e:
            add_log(task_id, f"Final link batch error: {e}")

    # Final save
    try:
        df.to_csv(temp_file, index=False, encoding='utf-8-sig')
        os.replace(temp_file, file_path)
    except Exception as e:
        add_log(task_id, f"Could not save final CSV: {e}")

    # Mark task as done
    task = get_task(task_id)
    if task:
        task['status'] = 'done'
        task['result_path'] = file_path
        add_log(task_id, f"Processing complete. Links added: {new_links}")