import re
import logging
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

VERSION_BLACKLIST = {
    "live", "cover", "remix", "acoustic", "reaction", "karaoke",
    "tutorial", "performance", "session", "unplugged", "edit",
    "version", "mv", "lyric", "Instrumental"
}

STOPWORDS = {'the', 'a', 'an', 'and', 'of', 'to', 'in', 'for', 'on',
             'with', 'by', 'is', 'that', 'it', 'from', 'at', 'as', 'this', 'be'}

def parse_duration(iso_duration):
    pattern = re.compile(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = pattern.match(iso_duration)
    if not match:
        return 0
    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0
    return hours * 3600 + minutes * 60 + seconds

def extract_significant_words(text):
    words = re.findall(r'\b\w+\b', text.lower())
    return [w for w in words if w not in STOPWORDS and len(w) > 1]

def title_contains_all_track_words(title, track_words):
    title_lower = title.lower()
    for word in track_words:
        if not re.search(rf'\b{re.escape(word)}\b', title_lower):
            return False
    return True

def is_blacklisted_version(title, track_name):
    title_lower = title.lower()
    track_lower = track_name.lower()
    for bw in VERSION_BLACKLIST:
        if bw in title_lower and bw not in track_lower:
            return True
    return False

def calculate_relevance_score(title, track_name, artist_name):
    title_lower = title.lower()
    track_lower = track_name.lower()
    artist_lower = artist_name.lower()

    score = 0
    if title_lower == f"{artist_lower} - {track_lower}" or title_lower == track_lower:
        score += 100
    if re.search(rf'\b{re.escape(track_lower)}\b', title_lower):
        score += 50
    if re.search(rf'\b{re.escape(artist_lower)}\b', title_lower):
        score += 30
    if "official" in title_lower:
        score += 20
    if "music video" in title_lower:
        score += 15
    title_words = set(re.findall(r'\b\w+\b', title_lower))
    expected_words = set(track_lower.split() + artist_lower.split())
    extra = len(title_words - expected_words)
    score -= extra * 2
    return score

def clean_artist(text):
    """Take only the first artist before common delimiters."""
    if not text:
        return ""
    for delim in [',', '-', '(', '[', 'feat.', 'ft.', '&']:
        if delim in text:
            text = text.split(delim)[0]
            break
    return text.strip()

def search_youtube(song, artist, album, api_key, target_duration_sec=None, max_results=50):
    """
    Search YouTube for a track.
    
    Returns:
        tuple: (youtube_url, duration_seconds, video_title) or (None, None, None) if not found.
    """
    if not api_key:
        raise ValueError("YouTube API key is missing")

    try:
        youtube = build('youtube', 'v3', developerKey=api_key)

        clean_artist_name = clean_artist(artist)
        query = f"{clean_artist_name} {song}".strip()

        logging.info(f"Searching YouTube with query: '{query}' (original artist: '{artist}')")

        search_response = youtube.search().list(
            q=query,
            part='snippet',
            maxResults=max_results,
            type='video'
        ).execute()

        items = search_response.get('items', [])
        if not items:
            logging.warning(f"No YouTube results for '{query}'")
            return None, None, None

        video_ids = []
        title_map = {}
        for item in items:
            video_id = item['id']['videoId']
            title = item['snippet']['title']
            video_ids.append(video_id)
            title_map[video_id] = title

        durations = {}
        for i in range(0, len(video_ids), 50):
            batch = video_ids[i:i+50]
            videos_response = youtube.videos().list(
                part='contentDetails',
                id=','.join(batch)
            ).execute()
            for video in videos_response.get('items', []):
                durations[video['id']] = parse_duration(video['contentDetails']['duration'])

        track_words = extract_significant_words(song)
        if not track_words:
            track_words = [w for w in re.findall(r'\b\w+\b', song.lower()) if len(w) > 1]

        # Strict pass
        strict_candidates = []
        for video_id, title in title_map.items():
            duration = durations.get(video_id)
            if duration is None:
                continue

            if target_duration_sec is not None and abs(duration - target_duration_sec) > 5:
                continue

            if not title_contains_all_track_words(title, track_words):
                continue

            if is_blacklisted_version(title, song):
                continue

            score = calculate_relevance_score(title, song, clean_artist_name)
            strict_candidates.append((video_id, title, duration, score))

        if strict_candidates:
            best = max(strict_candidates, key=lambda x: x[3])
            logging.info(f"Strict match for '{song}': '{best[1]}' (score {best[3]})")
            return f"https://www.youtube.com/watch?v={best[0]}", best[2], best[1]

        # Fallback
        logging.info(f"No strict match for '{song}'. Falling back.")
        fallback_candidates = []

        for video_id, title in title_map.items():
            duration = durations.get(video_id)
            if duration is None:
                continue

            if target_duration_sec is not None and duration > target_duration_sec + 10:
                continue

            title_lower = title.lower()
            word_matched = any(word in title_lower for word in track_words)
            if not word_matched:
                continue

            if is_blacklisted_version(title, song):
                continue

            score = 0
            if re.search(rf'\b{re.escape(clean_artist_name.lower())}\b', title_lower):
                score += 30
            if re.search(rf'\b{re.escape(song.lower())}\b', title_lower):
                score += 50
            if "official" in title_lower or "music video" in title_lower:
                score += 20
            title_words = set(re.findall(r'\b\w+\b', title_lower))
            expected_words = set(song.lower().split() + clean_artist_name.lower().split())
            extra = len(title_words - expected_words)
            score -= extra

            fallback_candidates.append((video_id, title, duration, score))

        if fallback_candidates:
            best = max(fallback_candidates, key=lambda x: x[3])
            logging.info(f"Fallback match for '{song}': '{best[1]}' (duration {best[2]}s, score {best[3]})")
            return f"https://www.youtube.com/watch?v={best[0]}", best[2], best[1]

        logging.info(f"No suitable YouTube match found for '{song}' by '{artist}'")
        return None, None, None

    except HttpError as e:
        if e.resp.status == 403:
            error_content = str(e)
            if "quotaExceeded" in error_content:
                raise Exception("YouTube API quota exceeded. Try again tomorrow.")
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