import pandas as pd
import time
import logging
import os
from datetime import datetime
from tasks import get_task, update_task
from local_cache import (
    init_dbs, is_track_seen, add_pending_track,
    get_cached_link, add_pending_link
)
from youtube import search_youtube
from backup_web import build_track_data_from_csv

# Column names in the input CSV
SONG_COL = "Track Name"
ARTIST_COL = "Artist Name(s)"
ALBUM_COL = "Album Name"
SPOTIFY_URI_COL = "Track URI"

# Output columns we add
YOUTUBE_LINK_COL = "YouTube Link"
YOUTUBE_DURATION_COL = "YouTube Duration"
YOUTUBE_TITLE_COL = "YouTube Title"

POSSIBLE_DURATION_COLS = ["Duration (ms)", "Track Duration (ms)"]


def add_log(task_id, msg):
    task = get_task(task_id)
    if task:
        logs = task.get('logs', [])
        logs.append(msg)
        update_task(task_id, logs=logs)


def process_csv(input_path, output_path, settings, username, task_id):
    # Initialize local databases
    init_dbs()

    # --- Read CSV with encoding fallback ---
    encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']
    df = None
    for enc in encodings:
        try:
            df = pd.read_csv(input_path, encoding=enc).dropna(how='all')
            add_log(task_id, f"Read CSV with {enc} encoding")
            break
        except UnicodeDecodeError:
            continue
    if df is None:
        add_log(task_id, "Error: Could not read CSV with any common encoding.")
        update_task(task_id, status='error')
        return

    update_task(task_id, progress=5)
    add_log(task_id, "CSV loaded. Checking existing tracks in local cache...")

    # --- Validate required columns ---
    required_cols = [SONG_COL, ARTIST_COL, SPOTIFY_URI_COL]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        add_log(task_id, f"Error: Missing required columns: {missing}")
        update_task(task_id, status='error')
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
    output_temp = output_path + ".tmp"
    quota_exhausted = False

    for idx, row in df.iterrows():
        # Check for pause/cancel
        task = get_task(task_id)
        if task and task.get('stop_requested'):
            add_log(task_id, "⏸️ Paused by user. Progress saved. You can resume later.")
            update_task(task_id, status='cancelled')
            try:
                # Save full dataframe (all columns) for resume
                df.to_csv(output_temp, index=False, encoding='utf-8-sig')
                os.replace(output_temp, output_path)
            except Exception:
                pass
            return

        # Link limit check
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

        # Target duration from CSV (milliseconds -> seconds)
        target_sec = None
        if duration_col and pd.notna(row.get(duration_col)):
            try:
                target_sec = float(row[duration_col]) / 1000.0
            except Exception:
                pass

        # --- Check local cache first ---
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
                link, api_dur, api_title = search_youtube(
                    song, artist, album, settings['api_key'],
                    target_duration_sec=target_sec
                )
            except Exception as e:
                error_msg = str(e).lower()
                if any(phrase in error_msg for phrase in ["quota exceeded", "quotaexceeded", "429", "per-minute"]):
                    add_log(task_id, f"🚨 QUOTA EXHAUSTED – stopping early: {e}")
                    quota_exhausted = True
                    break
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
                add_log(task_id, f"Inserted: {song}")

                # Build full row dict for the links table
                link_row = {
                    "song": song,
                    "artist": artist,
                    "spotify_uri": spotify_uri,
                    "youtube_link": link,
                    "username": username,
                    "duration_seconds": api_dur,
                    "fetched_at": datetime.utcnow().isoformat(),
                    "Album Name": album if album else "",
                    "title": api_title
                }
                add_pending_link(spotify_uri, link, api_dur, api_title, link_row)

                # Store track row in local cache (mapped to DB columns)
                if not is_track_seen(spotify_uri):
                    track_row_mapped = build_track_data_from_csv(row.to_dict())
                    if track_row_mapped:
                        add_pending_track(spotify_uri, track_row_mapped)

        # Ensure track is recorded even if no link found (again, use mapped row)
        if not is_track_seen(spotify_uri):
            track_row_mapped = build_track_data_from_csv(row.to_dict())
            if track_row_mapped:
                add_pending_track(spotify_uri, track_row_mapped)

        processed += 1

        # Periodic save of CSV progress (save full dataframe for resume)
        if processed % 10 == 0:
            try:
                df.to_csv(output_temp, index=False, encoding='utf-8-sig')
                os.replace(output_temp, output_path)
            except Exception as e:
                add_log(task_id, f"Warning: Could not save CSV: {e}")

        update_task(task_id, progress=int((processed / total_rows) * 100))
        time.sleep(settings['delay'])

        if quota_exhausted:
            break

    # --- FINAL SAVE: save the FULL dataframe (all original columns + YouTube columns) ---
    try:
        df.to_csv(output_temp, index=False, encoding='utf-8-sig')
        os.replace(output_temp, output_path)
        add_log(task_id, f"Saved final CSV with all {len(df.columns)} columns.")
    except Exception as e:
        add_log(task_id, f"Could not save final CSV: {e}")

    # Update task status
    task = get_task(task_id)
    if task and task.get('status') != 'cancelled':
        if quota_exhausted:
            update_task(task_id, status='error')
            add_log(task_id, "Processing stopped due to YouTube API quota exhaustion.")
        else:
            update_task(task_id, status='done')
            add_log(task_id, f"Processing complete. Links added: {new_links}")