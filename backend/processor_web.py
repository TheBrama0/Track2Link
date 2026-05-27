import pandas as pd
import time
import logging
import os
from tasks import get_task
from cache_web import get_cached_link, prepare_link_row, batch_insert_links, _supabase, _retry_on_connection
from backup_web import batch_upsert_tracks, build_track_data_from_csv
from youtube import search_youtube

SONG_COL = "Track Name"
ARTIST_COL = "Artist Name(s)"
ALBUM_COL = "Album Name"
SPOTIFY_URI_COL = "Track URI"
YOUTUBE_LINK_COL = "YouTube Link"
YOUTUBE_DURATION_COL = "YouTube Duration"
YOUTUBE_TITLE_COL = "YouTube Title"          # new column
POSSIBLE_DURATION_COLS = ["Duration (ms)", "Track Duration (ms)"]

def add_log(task_id, msg):
    task = get_task(task_id)
    if task:
        task['logs'].append(msg)

@_retry_on_connection
def get_existing_uris(uri_list, batch_size=100):
    existing = set()
    for i in range(0, len(uri_list), batch_size):
        batch_uris = uri_list[i:i+batch_size]
        res = _supabase.table("tracks").select("spotify_uri").in_("spotify_uri", batch_uris).execute()
        if res.data:
            existing.update(row["spotify_uri"] for row in res.data)
    return existing

def process_csv(file_path, settings, username, task_id):
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

    # --- Update progress: CSV loaded ---
    task = get_task(task_id)
    if task:
        task['progress'] = 5
    add_log(task_id, "CSV loaded. Checking existing tracks in database...")

    # --- Prepare track data and filter only NEW tracks ---
    tracks_data = build_track_data_from_csv(df)
    all_uris = [row["spotify_uri"] for row in tracks_data if row.get("spotify_uri")]

    # --- Try to fetch existing URIs with retries; if fails, stop and show error ---
    try:
        existing_uris = get_existing_uris(all_uris)
    except Exception as e:
        add_log(task_id, f"❌ Cannot connect to Supabase database: {e}")
        add_log(task_id, "Please check your internet connection and try again.")
        task = get_task(task_id)
        if task:
            task['status'] = 'error'
        return

    new_tracks = [row for row in tracks_data if row["spotify_uri"] not in existing_uris]

    if new_tracks:
        add_log(task_id, f"Backing up {len(new_tracks)} new tracks (skipped {len(existing_uris)} existing).")
        batch_size = 50
        total_batches = (len(new_tracks) + batch_size - 1) // batch_size
        for i in range(0, len(new_tracks), batch_size):
            batch = new_tracks[i:i+batch_size]
            batch_num = i // batch_size + 1
            try:
                batch_upsert_tracks(batch)
                add_log(task_id, f"Backup batch {batch_num}/{total_batches} done")
                time.sleep(0.2)
            except Exception as e:
                add_log(task_id, f"Backup error in batch {batch_num}: {e}")
                logging.exception("Backup batch failed")
        add_log(task_id, "Track backup complete (new tracks only).")
    else:
        add_log(task_id, "No new tracks to back up (all already in database).")

    # --- Update progress after backup ---
    if task:
        task['progress'] = 15

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
        df[YOUTUBE_DURATION_COL] = float('nan')
    else:
        df[YOUTUBE_DURATION_COL] = df[YOUTUBE_DURATION_COL].astype(float)
    if YOUTUBE_TITLE_COL not in df.columns:
        df[YOUTUBE_TITLE_COL] = ''

    total_rows = len(df)
    processed = 0
    new_links = 0
    limit_type = settings['limit_type']
    limit_value = settings['limit_value']

    link_batch = []
    temp_file = file_path + ".tmp"
    quota_exhausted = False   # flag to break outer loop

    for idx, row in df.iterrows():
        # --- Check if user requested pause/cancel ---
        task = get_task(task_id)
        if task and task.get('stop_requested'):
            add_log(task_id, "⏸️ Paused by user. Progress saved. You can resume later.")
            task['status'] = 'cancelled'
            # Save current state before exiting
            try:
                df.to_csv(temp_file, index=False, encoding='utf-8-sig')
                os.replace(temp_file, file_path)
            except Exception:
                pass
            return

        # --- Stop if we reached the link limit (based on new_links, not processed rows) ---
        if limit_type == 'rows' and limit_value > 0 and new_links >= limit_value:
            add_log(task_id, f"✅ Reached link limit ({limit_value} new links). Stopping.")
            break

        # Skip rows that already have a YouTube link
        existing_link = row.get(YOUTUBE_LINK_COL)
        if pd.notna(existing_link) and str(existing_link).startswith('http'):
            processed += 1
            continue

        song = str(row.get(SONG_COL, '')).strip()
        artist = str(row.get(ARTIST_COL, '')).strip()
        spotify_uri = str(row.get(SPOTIFY_URI_COL, '')).strip()

        # Extract album name
        album = None
        if ALBUM_COL in df.columns:
            album_val = row.get(ALBUM_COL)
            if pd.notna(album_val):
                album = str(album_val).strip()
                if album.lower() == 'nan':
                    album = None

        if not song or not artist or song.lower() == 'nan' or artist.lower() == 'nan':
            processed += 1
            continue
        if not spotify_uri or spotify_uri.lower() == 'nan':
            processed += 1
            continue

        # Target duration (in seconds) from CSV
        target_sec = None
        if duration_col and pd.notna(row.get(duration_col)):
            try:
                target_sec = float(row[duration_col]) / 1000.0
            except Exception:
                pass

        # Check cache (now returns 3 values)
        link, cached_dur, cached_title = get_cached_link(spotify_uri)
        if link:
            df.at[idx, YOUTUBE_LINK_COL] = link
            if cached_dur is not None:
                df.at[idx, YOUTUBE_DURATION_COL] = float(cached_dur)
            if cached_title:
                df.at[idx, YOUTUBE_TITLE_COL] = cached_title
            new_links += 1
            add_log(task_id, f"Inserted (cache): {song}")
        else:
            try:
                # search_youtube now returns (link, duration, title)
                link, api_dur, api_title = search_youtube(song, artist, album, settings['api_key'], target_duration_sec=target_sec)
            except Exception as e:
                error_msg = str(e).lower()
                # Check for any kind of quota exhaustion (daily or per-minute)
                if any(phrase in error_msg for phrase in ["quota exceeded", "quotaexceeded", "429", "per-minute"]):
                    add_log(task_id, f"🚨 QUOTA EXHAUSTED – stopping early: {e}")
                    quota_exhausted = True
                    break   # exit the for loop immediately
                else:
                    add_log(task_id, f"Error searching {song}: {e}")
                link = None
                api_dur = None
                api_title = None

            if link:
                df.at[idx, YOUTUBE_LINK_COL] = link
                if api_dur is not None:
                    df.at[idx, YOUTUBE_DURATION_COL] = float(api_dur)
                if api_title:
                    df.at[idx, YOUTUBE_TITLE_COL] = api_title
                new_links += 1

                link_batch.append(prepare_link_row(song, artist, spotify_uri,
                                                   link, username, api_dur, album, api_title))
                add_log(task_id, f"Inserted: {song}")

                if len(link_batch) >= 10:
                    try:
                        batch_insert_links(link_batch)
                        add_log(task_id, f"Flushed {len(link_batch)} links to database")
                    except Exception as e:
                        add_log(task_id, f"Link cache error (batch flush): {e}")
                    link_batch = []

                time.sleep(settings['delay'])

        processed += 1

        if processed % 10 == 0:
            try:
                df.to_csv(temp_file, index=False, encoding='utf-8-sig')
                os.replace(temp_file, file_path)
            except Exception as e:
                add_log(task_id, f"Warning: Could not save CSV: {e}")

        task = get_task(task_id)
        if task:
            task['progress'] = int((processed / total_rows) * 100)

        if quota_exhausted:
            break

    # Flush remaining links
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

    task = get_task(task_id)
    if task and task['status'] != 'cancelled':
        if quota_exhausted:
            task['status'] = 'error'   # mark as error so user knows quota was the issue
            add_log(task_id, "Processing stopped due to YouTube API quota exhaustion.")
        else:
            task['status'] = 'done'
        task['result_path'] = file_path
        add_log(task_id, f"Processing complete. Links added: {new_links}")