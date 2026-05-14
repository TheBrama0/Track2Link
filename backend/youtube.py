import re
import logging
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

BASE_BLACKLIST = {
    "live", "cover", "remix", "acoustic", "reaction", "karaoke",
    "tutorial", "performance", "session", "unplugged", "edit",
    "version", "mv"
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

def _get_filtered_words(song):
    song_lower = song.lower()
    return [word for word in BASE_BLACKLIST if word not in song_lower]

def is_blacklisted(title, filtered_words):
    title_lower = title.lower()
    return any(word in title_lower for word in filtered_words)

def calculate_relevance_score(title, song, artist):
    """
    Returns a relevance score (higher = better) for a video title.
    """
    title_lower = title.lower()
    song_lower = song.lower()
    artist_lower = artist.lower()
    score = 0

    # Exact title match (whole title equals song+artist or just song)
    if title_lower == f"{artist_lower} - {song_lower}" or title_lower == song_lower:
        score += 100
    # Song name appears as a whole word (using word boundaries)
    # Simple substring with spaces to avoid partial matches (e.g., "style" vs "styl")
    if re.search(rf'\b{re.escape(song_lower)}\b', title_lower):
        score += 50
    elif song_lower in title_lower:
        score += 30

    # Artist name appears
    if re.search(rf'\b{re.escape(artist_lower)}\b', title_lower):
        score += 40
    elif artist_lower in title_lower:
        score += 20

    # Penalize extra words that are not in song or artist
    title_words = set(re.findall(r'\w+', title_lower))
    song_words = set(re.findall(r'\w+', song_lower))
    artist_words = set(re.findall(r'\w+', artist_lower))
    extra_words = title_words - song_words - artist_words - {"official", "music", "video", "audio", "hd", "lyric", "vevo", "official"}
    # Penalty per extra word (small)
    score -= len(extra_words) * 2

    # Bonus for "official" or "music video" in title
    if "official" in title_lower:
        score += 15
    if "music video" in title_lower:
        score += 10

    return score

def search_youtube(song, artist, api_key, target_duration_sec=None, max_results=15):
    if not api_key:
        raise ValueError("YouTube API key is missing")

    try:
        youtube = build('youtube', 'v3', developerKey=api_key)
        query = f"{song} {artist}"

        search_response = youtube.search().list(
            q=query,
            part='snippet',
            maxResults=max_results,
            type='video'
        ).execute()

        items = search_response.get('items', [])
        if not items:
            return None, None

        filtered_words = _get_filtered_words(song)

        candidates = []
        for item in items:
            video_id = item['id']['videoId']
            title = item['snippet']['title']
            # Skip blacklisted
            if is_blacklisted(title, filtered_words):
                continue
            # Compute relevance score
            score = calculate_relevance_score(title, song, artist)
            candidates.append((video_id, title, score))

        if not candidates:
            # No filtered results – fall back to all (without blacklist check)
            candidates = [(item['id']['videoId'], item['snippet']['title'], 0) for item in items]

        if not candidates:
            return None, None

        # Sort by relevance score descending, then by position (first result wins tie)
        candidates.sort(key=lambda x: x[2], reverse=True)

        # Fetch durations for the top 5 (or 8) candidates to also consider duration
        top_candidates = candidates[:8]
        video_ids = [vid for vid, _, _ in top_candidates]

        videos_response = youtube.videos().list(
            part='contentDetails',
            id=','.join(video_ids)
        ).execute()
        duration_map = {}
        for video in videos_response.get('items', []):
            duration_map[video['id']] = parse_duration(video['contentDetails']['duration'])

        # If target duration is provided, incorporate duration difference into final score
        if target_duration_sec is not None:
            best_score = -float('inf')
            best_vid = None
            best_duration = None
            for vid, title, rel_score in top_candidates:
                dur = duration_map.get(vid)
                if dur is None:
                    continue
                # Duration difference penalty (normalized to 0-20 points)
                diff_penalty = min(20, abs(dur - target_duration_sec) / 5.0)  # 5 sec diff = 1 point
                total_score = rel_score - diff_penalty
                if total_score > best_score:
                    best_score = total_score
                    best_vid = vid
                    best_duration = dur
            if best_vid:
                return f"https://www.youtube.com/watch?v={best_vid}", best_duration

        # No target duration or no duration info – return highest relevance score
        best_vid = candidates[0][0]
        best_duration = duration_map.get(best_vid, 0)
        return f"https://www.youtube.com/watch?v={best_vid}", best_duration

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