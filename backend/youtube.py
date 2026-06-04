import re
import logging
import time
import random
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

VERSION_BLACKLIST = {
    "live", "cover", "remix", "acoustic", "reaction", "karaoke",
    "tutorial", "performance", "session", "unplugged", "edit",
    "version", "mv", "lyric", "instrumental"
}

def parse_duration(iso_duration):
    pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = pattern.match(iso_duration)
    if not match:
        return 0
    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0
    return hours * 3600 + minutes * 60 + seconds

def is_blacklisted_version(title, track_name):
    title_lower = title.lower()
    track_lower = track_name.lower()
    for bw in VERSION_BLACKLIST:
        if bw in title_lower and bw not in track_lower:
            return True
    return False

def clean_artist(text):
    """Take only the first artist before common delimiters."""
    if not text:
        return ""
    for delim in [',', '-', '(', '[', 'feat.', 'ft.', '&']:
        if delim in text:
            text = text.split(delim)[0]
            break
    return text.strip()

def search_youtube(song, artist, album, api_key, target_duration_sec=None, max_results=50, retries=5):
    if not api_key:
        raise ValueError("YouTube API key is missing")

    clean_artist_name = clean_artist(artist)
    # Build query and simplify: remove parentheses and dash suffix
    query = f"{clean_artist_name} {song}".strip()
    query = re.sub(r'\s*\([^)]*\)', '', query)   # remove (anything)
    query = re.sub(r'\s*\-.*$', '', query)       # remove dash and after
    query = re.sub(r'\s+', ' ', query).strip()
    logging.info(f"Search query: '{query}'")

    # Extract allowed words (song + artist + album)
    song_words = re.findall(r'\b\w+\b', song.lower())
    artist_words = re.findall(r'\b\w+\b', clean_artist_name.lower())
    album_words = re.findall(r'\b\w+\b', album.lower()) if album and album.strip() and album.lower() != 'nan' else []
    allowed_words = set(song_words + artist_words + album_words)

    for attempt in range(retries):
        try:
            youtube = build('youtube', 'v3', developerKey=api_key)
            # Single search request – only 50 results to save quota
            search_response = youtube.search().list(
                q=query,
                part='snippet',
                maxResults=50,           # only one page, max 50 results
                type='video',
                videoCategoryId='10',    # music only
                order='viewCount',        # most viewed first
                regionCode='US'
            ).execute()

            items = search_response.get('items', [])
            if not items:
                logging.warning(f"No YouTube results for '{query}'")
                return None, None, None

            video_metadata = []
            video_ids = []
            for item in items:
                video_id = item['id']['videoId']
                title = item['snippet']['title']
                channel_title = item['snippet']['channelTitle']
                video_ids.append(video_id)
                video_metadata.append((video_id, title, channel_title))

            # Fetch durations
            durations = {}
            for i in range(0, len(video_ids), 50):
                batch = video_ids[i:i+50]
                videos_response = youtube.videos().list(
                    part='contentDetails',
                    id=','.join(batch)
                ).execute()
                for video in videos_response.get('items', []):
                    durations[video['id']] = parse_duration(video['contentDetails']['duration'])

            # ----- Heavy scoring (penalty for extra words) -----
            def score_video_heavy(title, channel_title):
                title_lower = title.lower()
                channel_lower = channel_title.lower()
                matched = 0
                for w in song_words:
                    if re.search(rf'\b{re.escape(w)}\b', title_lower):
                        matched += 1
                title_words = set(re.findall(r'\b\w+\b', title_lower))
                extra = len(title_words - allowed_words)
                score = min(250, matched * 50)
                score -= extra * 10
                if re.search(rf'\b{re.escape(clean_artist_name.lower())}\b', title_lower):
                    score += 50
                if re.search(rf'\b{re.escape(clean_artist_name.lower())}\b', channel_lower):
                    score += 20
                if "official" in title_lower or "music video" in title_lower:
                    score += 20
                if title_lower == f"{clean_artist_name.lower()} - {song.lower()}":
                    score += 100
                return score, matched

            # ----- Light scoring (no penalty for extra words) -----
            def score_video_light(title, channel_title):
                title_lower = title.lower()
                channel_lower = channel_title.lower()
                matched = 0
                for w in song_words:
                    if re.search(rf'\b{re.escape(w)}\b', title_lower):
                        matched += 1
                score = min(200, matched * 50)
                if re.search(rf'\b{re.escape(clean_artist_name.lower())}\b', title_lower):
                    score += 50
                if re.search(rf'\b{re.escape(clean_artist_name.lower())}\b', channel_lower):
                    score += 20
                if "official" in title_lower or "music video" in title_lower:
                    score += 20
                if title_lower == f"{clean_artist_name.lower()} - {song.lower()}":
                    score += 100
                return score, matched

            # ----- PASS 1 (Strict) -----
            # - Artist must appear in title or channel
            # - Every word in title must be in allowed_words
            candidates1 = []
            for video_id, title, channel_title in video_metadata:
                duration = durations.get(video_id)
                if duration is None:
                    continue
                if is_blacklisted_version(title, song):
                    continue
                title_lower = title.lower()
                channel_lower = channel_title.lower()
                artist_in_title = bool(re.search(rf'\b{re.escape(clean_artist_name.lower())}\b', title_lower))
                artist_in_channel = bool(re.search(rf'\b{re.escape(clean_artist_name.lower())}\b', channel_lower))
                if not (artist_in_title or artist_in_channel):
                    continue
                title_words = set(re.findall(r'\b\w+\b', title_lower))
                if not title_words.issubset(allowed_words):
                    continue
                score, matched = score_video_heavy(title, channel_title)
                candidates1.append((video_id, title, duration, score, matched, channel_title))

            if candidates1:
                candidates1.sort(key=lambda x: (-x[3], abs(x[2] - target_duration_sec) if target_duration_sec else 0))
                best = candidates1[0]
                logging.info(f"Strict match for '{song}': '{best[1]}' (score {best[3]}, matched {best[4]})")
                return f"https://www.youtube.com/watch?v={best[0]}", best[2], best[1]

            # ----- PASS 2 (Mid) -----
            # - Artist must appear in title or channel
            # - No unknown-word filter
            logging.info(f"No strict match. Trying mid pass (artist required, light scoring).")
            candidates2 = []
            for video_id, title, channel_title in video_metadata:
                duration = durations.get(video_id)
                if duration is None:
                    continue
                if is_blacklisted_version(title, song):
                    continue
                title_lower = title.lower()
                channel_lower = channel_title.lower()
                artist_in_title = bool(re.search(rf'\b{re.escape(clean_artist_name.lower())}\b', title_lower))
                artist_in_channel = bool(re.search(rf'\b{re.escape(clean_artist_name.lower())}\b', channel_lower))
                if not (artist_in_title or artist_in_channel):
                    continue
                score, matched = score_video_light(title, channel_title)
                candidates2.append((video_id, title, duration, score, matched, channel_title))

            if candidates2:
                candidates2.sort(key=lambda x: (-x[3], abs(x[2] - target_duration_sec) if target_duration_sec else 0))
                best = candidates2[0]
                logging.info(f"Mid match for '{song}': '{best[1]}' (score {best[3]}, matched {best[4]})")
                return f"https://www.youtube.com/watch?v={best[0]}", best[2], best[1]

            logging.info(f"No suitable YouTube match found for '{song}' by '{artist}'")
            return None, None, None

        except HttpError as e:
            if e.resp.status == 429:
                base_wait = min(60, (2 ** attempt) + random.uniform(0, 1))
                wait = base_wait + random.uniform(0, 1)
                logging.warning(f"Rate limited (429). Retrying in {wait:.2f}s... (attempt {attempt+1}/{retries})")
                time.sleep(wait)
                if attempt == retries - 1:
                    raise Exception("YouTube API per‑minute quota exceeded after retries.")
                continue
            elif e.resp.status == 403:
                error_content = str(e)
                if "quotaExceeded" in error_content:
                    raise Exception("YouTube API daily quota exceeded. Try again tomorrow.")
                elif "accessNotConfigured" in error_content or "API key not valid" in error_content:
                    raise Exception("Invalid YouTube API key or API not enabled.")
                else:
                    raise Exception("YouTube API permission error. Check your API key.")
            elif e.resp.status == 400:
                raise Exception("Bad request to YouTube API. Check your query.")
            elif e.resp.status == 404:
                raise Exception("YouTube API endpoint not found. This may be a temporary issue.")
            else:
                raise Exception(f"YouTube API error (HTTP {e.resp.status}): {e.reason}")
        except Exception as e:
            logging.exception("Unexpected error in YouTube search")
            raise Exception(f"YouTube search failed: {str(e)}")

    return None, None, None
