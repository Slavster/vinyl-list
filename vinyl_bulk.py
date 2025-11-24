# vinyl_bulk.py — Sync Vision, no GCS outputs, Discogs retries, error capture, and de-dup against your collection
# Deps: pip install google-cloud-vision google-cloud-storage pandas requests
#
# Required env:
#   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/vinyl-vision-2.json
#   export DISCOGS_USER=your_username
#   export DISCOGS_TOKEN=your_discogs_token
#
# Optional env (helps Discogs trust your client):
#   export DISCOGS_APP_NAME="vinyl-bulk"
#   export DISCOGS_APP_VERSION="1.1"
#   export DISCOGS_CONTACT="Your Name <you@example.com>"
#   export DISCOGS_APP_URL="https://github.com/yourrepo"

import os, re, time, posixpath, random, json, argparse
from functools import lru_cache
from urllib.parse import urlparse
from datetime import datetime

import requests
import pandas as pd
try:
    import spotipy
    from spotipy.oauth2 import SpotifyOAuth
    SPOTIPY_AVAILABLE = True
except ImportError:
    SPOTIPY_AVAILABLE = False
from google.cloud import vision, storage
from google.cloud.exceptions import NotFound, Forbidden
from google.auth.exceptions import DefaultCredentialsError
from google.protobuf.json_format import MessageToDict

# --- Auto-load .env early (never overwrite real env) ---
try:
    from dotenv import load_dotenv, find_dotenv
except ModuleNotFoundError:
    raise SystemExit(
        "Missing dependency: python-dotenv. Install it with:\n"
        "  pip install python-dotenv"
    )

# Load order: .env.local (highest), .env (fallback). Do NOT override existing env.
# This lets prod/CI env vars win, and local shells still “just work”.
load_dotenv(".env.local", override=False)
load_dotenv(find_dotenv(filename=".env", usecwd=True), override=False)

# Optional: fail fast on required vars and show where they came from.
REQUIRED_ENVS = ["VINYL_GCS_BUCKET", "DISCOGS_USER", "DISCOGS_TOKEN", "GOOGLE_APPLICATION_CREDENTIALS"]

missing = [k for k in REQUIRED_ENVS if not os.getenv(k)]
if missing:
    raise SystemExit(
        "Missing required environment variables: "
        + ", ".join(missing)
        + "\nSet them in your shell or in .env/.env.local"
    )

# Expand GOOGLE_APPLICATION_CREDENTIALS path (handles $HOME, ~, etc.)
creds_env = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if creds_env:
    creds_expanded = os.path.expanduser(os.path.expandvars(creds_env))
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_expanded

# ========= CONFIG =========
GCS_BUCKET         = os.getenv("VINYL_GCS_BUCKET", "your-bucket").strip()
INPUT_PREFIX       = os.getenv("VINYL_INPUT_PREFIX", "covers/").strip()
DISCOGS_USER       = os.environ.get("DISCOGS_USER")
DISCOGS_TOKEN      = os.environ.get("DISCOGS_TOKEN")
DISCOGS_FOLDER_ID  = int(os.getenv("DISCOGS_FOLDER_ID", "1"))              # 1 = Uncategorized
VISION_SYNC_CHUNK  = int(os.getenv("VISION_SYNC_CHUNK", "8"))              # <=16; use 4–8 if images are large
# Default conditions for collection items
DISCOGS_MEDIA_CONDITION   = os.getenv("DISCOGS_MEDIA_CONDITION", "Very Good (VG)").strip()
DISCOGS_SLEEVE_CONDITION  = os.getenv("DISCOGS_SLEEVE_CONDITION", "Good Plus (G+)").strip()
# Matching preferences
FORMAT_FILTER = os.getenv("FORMAT_FILTER", "Vinyl").strip()
COUNTRY_PREF = os.getenv("COUNTRY_PREF", "US").strip()
SEARCH_PAGE_SIZE = int(os.getenv("SEARCH_PAGE_SIZE", "10"))
# Spotify playlist builder
DISCOGS_PLAYLIST_SOURCE_FOLDER = os.getenv("DISCOGS_PLAYLIST_SOURCE_FOLDER", "").strip()
SPOTIFY_PLAYLIST_URL = os.getenv("SPOTIFY_PLAYLIST_URL", "").strip()
if not GCS_BUCKET:
    raise SystemExit("VINYL_GCS_BUCKET not set (export it or add it to .env/.env.local).")
# ======================================

# ---- Optional Discogs UA identity ----
DISCOGS_APP_NAME    = os.getenv("DISCOGS_APP_NAME", "vinyl-bulk")
DISCOGS_APP_VERSION = os.getenv("DISCOGS_APP_VERSION", "1.0")
DISCOGS_CONTACT     = os.getenv("DISCOGS_CONTACT", "").strip()
DISCOGS_APP_URL     = os.getenv("DISCOGS_APP_URL", "").strip()


# ----------------- Helpers -----------------

def gcs_uri(obj: str) -> str:
    return f"gs://{GCS_BUCKET}/{obj}"

def filename_from_gcs_uri(uri: str) -> str:
    if not uri:
        return ""
    p = urlparse(uri)
    return posixpath.basename(p.path)

def owner_from_gcs_uri(uri: str) -> str:
    """Extract folder name from gs://bucket/covers/<Owner>/<Subfolder>/file.jpg
    Returns 'Owner_Subfolder' format, joining all subdirectories with underscores.
    Example: gs://bucket/covers/Dad/Shed/image.jpg -> 'Dad_Shed'
    """
    if not uri:
        return ""
    p = urlparse(uri)
    path = p.path.lstrip("/")
    if path.startswith(GCS_BUCKET + "/"):
        path = path[len(GCS_BUCKET) + 1:]
    rel = path[len(INPUT_PREFIX):] if path.startswith(INPUT_PREFIX) else path
    
    # Split path into components and remove the filename (last component)
    parts = [p for p in rel.split("/") if p]  # Filter out empty strings
    if not parts:
        return ""
    
    # If there's only one component, it's the filename (no folders)
    # If there are 2+ components, the last is the filename, the rest are folders
    if len(parts) == 1:
        # No folders, just a filename directly in INPUT_PREFIX
        return ""
    
    # Remove filename (last component) and keep directory components
    folder_parts = parts[:-1]
    
    # Join all folder components with underscores
    return "_".join(folder_parts) if folder_parts else ""

def extract_release_or_master(url: str):
    """Return ('release'|'master', id) if URL matches Discogs structure."""
    try:
        path = urlparse(url).path
        m = re.search(r"/release/(\d+)", path)
        if m:
            return ("release", int(m.group(1)))
        m = re.search(r"/master/(\d+)", path)
        if m:
            return ("master", int(m.group(1)))
    except Exception:
        pass
    return (None, None)

def split_top_candidate_urls(web: dict, limit=3):
    """Discogs-first candidates + others (deduped, order preserved)."""
    urls = [p.get("url") for p in web.get("pagesWithMatchingImages", []) if p.get("url")]
    seen, dedup = set(), []
    for u in urls:
        if u not in seen:
            dedup.append(u); seen.add(u)
    discogs = [u for u in dedup if "discogs.com" in u.lower()][:limit]
    other   = [u for u in dedup if "discogs.com" not in u.lower()][:limit]
    return discogs, other

def confidence_bucket(method: str, has_discogs_candidates: bool, is_vinyl: bool = True, is_us: bool = True):
    """
    Determine confidence level based on match method and validation results.
    Updated to reflect vinyl/US preference.
    """
    if method == "release_url" and is_vinyl and is_us:
        return "high"
    if method == "master_url" and is_vinyl and is_us:
        return "medium"
    if method == "master_url" and is_vinyl:
        return "medium"  # Vinyl but not US
    if method == "search_fallback" and is_vinyl and is_us and has_discogs_candidates:
        return "low"
    if method == "search_fallback" and is_vinyl:
        return "very_low"  # Vinyl but not US, or no direct URL
    if method == "search_fallback":
        return "very_low"
    return "unknown"


# ----------------- Vision Results Cache -----------------

VISION_CACHE_FILE = "vision_results.json"

def load_vision_cache():
    """Load previously saved Vision API results from JSON file."""
    if os.path.exists(VISION_CACHE_FILE):
        try:
            with open(VISION_CACHE_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            print(f"Warning: Could not load vision cache: {e}. Starting fresh.")
            return {}
    return {}

def save_vision_cache(cache):
    """Save Vision API results to JSON file."""
    try:
        with open(VISION_CACHE_FILE, 'w') as f:
            json.dump(cache, f, indent=2)
        print(f"Saved Vision results for {len(cache)} images to {VISION_CACHE_FILE}")
    except Exception as e:
        print(f"Warning: Could not save vision cache: {e}")

def get_vision_result(cache, uri):
    """Get cached Vision result for a specific image URI."""
    return cache.get(uri)

def set_vision_result(cache, uri, result):
    """Store Vision result for a specific image URI."""
    cache[uri] = result


# ---- Discogs headers (private-friendly UA) ----
def discogs_headers():
    # Core must not be empty; Discogs requires a UA string.
    name = (DISCOGS_APP_NAME or "vinyl-bulk").strip()
    ver  = (DISCOGS_APP_VERSION or "1.0").strip()
    ua_core = f"{name}/{ver}" if ver else name

    extras = []
    if DISCOGS_APP_URL:   # optional
        extras.append(f"+{DISCOGS_APP_URL}")
    if DISCOGS_CONTACT:   # optional
        extras.append(f"contact: {DISCOGS_CONTACT}")

    ua = ua_core if not extras else f"{ua_core} ({'; '.join(extras)})"

    headers = {
        "User-Agent": ua,
        "Accept": "application/json",
    }
    if DISCOGS_TOKEN:
        headers["Authorization"] = f"Discogs token={DISCOGS_TOKEN}"
    return headers


# ----------------- HTTP w/ retry -----------------

def http_get_with_retry(url, *, params=None, headers=None, timeout=20, tries=4, base_delay=0.8, context=None):
    """
    HTTP GET with retry logic.
    context: Optional string to include in retry messages (e.g., "image 5/221")
    """
    for attempt in range(1, tries + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504):
                # For 429, check for Retry-After header
                if r.status_code == 429:
                    retry_after = r.headers.get("Retry-After")
                    if retry_after:
                        try:
                            delay = int(retry_after) + random.uniform(0, 1)
                        except (ValueError, TypeError):
                            delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
                    else:
                        # Exponential backoff with jitter, longer for 429
                        delay = base_delay * (2 ** (attempt - 1)) * 2 + random.uniform(0, 1)
                    
                    if attempt < tries:
                        context_str = f" [{context}]" if context else ""
                        print(f"GET retry {attempt}/{tries-1} after 429 rate limit{context_str} (sleep {delay:.1f}s)")
                        time.sleep(delay)
                        continue
                    else:
                        raise requests.HTTPError(f"Transient {r.status_code}", response=r)
                else:
                    raise requests.HTTPError(f"Transient {r.status_code}", response=r)
            r.raise_for_status()
            return r
        except requests.HTTPError as e:
            if attempt == tries:
                raise
            # For non-429 errors, use standard exponential backoff
            if e.response and e.response.status_code != 429:
                delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
                context_str = f" [{context}]" if context else ""
                print(f"GET retry {attempt}/{tries-1} after error: {e}{context_str} (sleep {delay:.1f}s)")
                time.sleep(delay)
        except Exception as e:
            if attempt == tries:
                raise
            delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
            context_str = f" [{context}]" if context else ""
            print(f"GET retry {attempt}/{tries-1} after error: {e}{context_str} (sleep {delay:.1f}s)")
            time.sleep(delay)

def http_post_with_retry(url, *, headers=None, json_data=None, timeout=20, tries=4, base_delay=0.8):
    for attempt in range(1, tries + 1):
        try:
            r = requests.post(url, headers=headers, json=json_data, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"Transient {r.status_code}", response=r)
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt == tries:
                raise
            delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
            print(f"POST retry {attempt}/{tries-1} after error: {e} (sleep {delay:.1f}s)")
            time.sleep(delay)

def http_put_with_retry(url, *, headers=None, json_data=None, timeout=20, tries=4, base_delay=0.8):
    for attempt in range(1, tries + 1):
        try:
            r = requests.put(url, headers=headers, json=json_data, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"Transient {r.status_code}", response=r)
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt == tries:
                raise
            delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 0.25)
            print(f"PUT retry {attempt}/{tries-1} after error: {e} (sleep {delay:.1f}s)")
            time.sleep(delay)


# ----------------- Discogs API -----------------

def discogs_get_release(release_id: int, context=None):
    """Fetch a release and return its data. Returns None on errors."""
    try:
        r = http_get_with_retry(f"https://api.discogs.com/releases/{release_id}",
                                headers=discogs_headers(), timeout=20, tries=6, context=context)
        time.sleep(0.6)  # Rate limiting
        return r.json()
    except Exception as e:
        context_str = f" [{context}]" if context else ""
        print(f"Failed to fetch release {release_id}{context_str}: {e}")
        return None

def validate_release_is_vinyl_and_us(release_data: dict):
    """
    Validate that a release is vinyl and preferably US.
    Returns tuple (is_vinyl: bool, is_us: bool, reason: str).
    """
    if not release_data:
        return (False, False, "No release data")
    
    formats = release_data.get("formats", [])
    is_vinyl = False
    format_names = []
    
    for fmt in formats:
        fmt_name = (fmt.get("name") or "").strip()
        format_names.append(fmt_name)
        if fmt_name.lower() == "vinyl":
            is_vinyl = True
            break
    
    if not is_vinyl:
        return (False, False, f"Not vinyl (formats: {', '.join(format_names)})")
    
    country = (release_data.get("country") or "").strip()
    is_us = country.upper() == "US"
    
    if is_us:
        return (True, True, "Vinyl, US")
    elif country:
        return (True, False, f"Vinyl, {country} (not US)")
    else:
        return (True, False, "Vinyl, country not specified")

def discogs_search(artist=None, title=None, catno=None, barcode=None, year=None, context=None):
    """
    Discogs search with vinyl and US preference.
    Returns list of results (up to SEARCH_PAGE_SIZE) or empty list on errors.
    """
    params = {
        "type": "release",
        "format": FORMAT_FILTER,
        "country": COUNTRY_PREF,
        "per_page": SEARCH_PAGE_SIZE
    }
    if artist:  params["artist"] = artist
    if title:   params["release_title"] = title
    if catno:   params["catno"] = catno
    if barcode: params["barcode"] = barcode
    if year:    params["year"] = str(year)

    try:
        r = http_get_with_retry("https://api.discogs.com/database/search",
                                params=params, headers=discogs_headers(), timeout=20, tries=6, context=context)
        res = r.json().get("results", [])
        time.sleep(0.6)  # Small delay to avoid rate limiting
        return res
    except Exception as e:
        # Log but don't crash - return empty list so the record is marked as review_needed
        context_str = f" [{context}]" if context else ""
        print(f"Discogs search failed{context_str} (will mark as review_needed): {e}")
        return []

def discogs_release_from_master(master_id: int, context=None):
    """
    Resolve a master ID to a concrete release with vinyl and US preference.
    Prefer main_release if it's vinyl+US; otherwise search versions with filters.
    Returns (release_id, is_vinyl, is_us, reason) or (None, False, False, reason) on failure.
    """
    try:
        r = http_get_with_retry(f"https://api.discogs.com/masters/{master_id}",
                                headers=discogs_headers(), timeout=20, tries=6, context=context)
        js = r.json()
    except Exception as e:
        context_str = f" [{context}]" if context else ""
        print(f"Failed to resolve master {master_id}{context_str}: {e}")
        return (None, False, False, f"Failed to fetch master: {e}")

    # Check main_release first
    main_release_id = js.get("main_release")
    if main_release_id:
        release_data = discogs_get_release(main_release_id, context=context)
        if release_data:
            is_vinyl, is_us, reason = validate_release_is_vinyl_and_us(release_data)
            if is_vinyl and is_us:
                return (main_release_id, True, True, f"Main release: {reason}")
            elif is_vinyl:
                # Keep as candidate but continue searching for US version
                best_candidate = (main_release_id, True, False, f"Main release: {reason}")
            else:
                best_candidate = None
        else:
            best_candidate = None
    else:
        best_candidate = None

    # Search versions - fetch all and filter client-side (API may not support format/country filters)
    vurl = js.get("versions_url") or f"https://api.discogs.com/masters/{master_id}/versions"
    params = {
        "per_page": 100,
        "page": 1
    }

    try:
        while True:
            vr = http_get_with_retry(vurl, headers=discogs_headers(), params=params, timeout=30, tries=6, context=context)
            vjs = vr.json()
            versions = vjs.get("versions", [])

            for v in versions:
                version_id = v.get("id")
                if not version_id:
                    continue
                
                # Fetch and validate this version
                version_data = discogs_get_release(version_id, context=context)
                if version_data:
                    is_vinyl, is_us, reason = validate_release_is_vinyl_and_us(version_data)
                    if is_vinyl and is_us:
                        # Perfect match - return immediately
                        return (version_id, True, True, f"Version: {reason}")
                    elif is_vinyl and not best_candidate:
                        # Keep as fallback if we don't have a candidate yet
                        best_candidate = (version_id, True, False, f"Version: {reason}")

            pg = vjs.get("pagination", {})
            if pg.get("page", 1) < pg.get("pages", 1):
                params["page"] = pg["page"] + 1
                time.sleep(0.5)  # Small delay to avoid rate limiting
                continue
            break
    except Exception as e:
        print(f"Failed to fetch versions for master {master_id}: {e}")

    # Return best candidate found, or None
    if best_candidate:
        return best_candidate
    return (None, False, False, "No vinyl releases found in master")

@lru_cache(maxsize=1)
def discogs_get_collection_field_ids(username: str):
    """
    Discover field IDs for Media Condition and Sleeve Condition.
    Returns dict with 'media_condition' and 'sleeve_condition' field IDs.
    """
    url = f"https://api.discogs.com/users/{username}/collection/fields"
    r = http_get_with_retry(url, headers=discogs_headers(), timeout=20)
    fields = r.json().get("fields", [])
    
    field_ids = {}
    for field in fields:
        name = field.get("name", "")
        field_id = field.get("id")
        # Match field names (case-insensitive, handle variations)
        name_lower = name.lower()
        if "media condition" in name_lower:
            field_ids["media_condition"] = field_id
        elif "sleeve condition" in name_lower:
            field_ids["sleeve_condition"] = field_id
    
    # Debug output if fields not found
    if not field_ids.get("media_condition") or not field_ids.get("sleeve_condition"):
        print(f"Warning: Could not find all condition fields. Available fields: {[f.get('name') for f in fields]}")
        print(f"Found field IDs: {field_ids}")
        # Print all fields for debugging
        print(f"All available fields: {[(f.get('id'), f.get('name')) for f in fields]}")
    
    return field_ids

def discogs_add_to_collection(username: str, release_id: int, folder_id: int):
    """
    Add a release to Discogs collection. Returns the instance_id from the response.
    Note: Conditions are set separately after adding.
    """
    url = f"https://api.discogs.com/users/{username}/collection/folders/{folder_id}/releases/{release_id}"
    headers = discogs_headers()
    
    r = http_post_with_retry(url, headers=headers, json_data=None, timeout=20)
    # Parse response to get instance_id
    response_data = r.json() if r.content else {}
    instance_id = response_data.get("instance_id") or response_data.get("id")
    return instance_id

# ---- Collection listing (for de-dup) ----

@lru_cache(maxsize=32)
def discogs_get_collection_folders(username: str):
    """Return a list of folder IDs in the user's collection."""
    r = http_get_with_retry(f"https://api.discogs.com/users/{username}/collection/folders",
                            headers=discogs_headers(), timeout=20)
    js = r.json()
    return [f["id"] for f in js.get("folders", [])]

@lru_cache(maxsize=32)
def discogs_get_collection_folders_with_names(username: str):
    """Return a dict mapping folder names to folder IDs."""
    r = http_get_with_retry(f"https://api.discogs.com/users/{username}/collection/folders",
                            headers=discogs_headers(), timeout=20)
    js = r.json()
    return {f["name"]: f["id"] for f in js.get("folders", [])}

def discogs_create_folder(username: str, folder_name: str):
    """
    Create a new folder in the Discogs collection.
    Returns the folder_id of the created folder, or None on error.
    """
    url = f"https://api.discogs.com/users/{username}/collection/folders"
    headers = discogs_headers()
    headers["Content-Type"] = "application/json"
    
    try:
        r = http_post_with_retry(url, headers=headers, json_data={"name": folder_name}, timeout=20)
        response_data = r.json() if r.content else {}
        folder_id = response_data.get("id")
        return folder_id
    except Exception as e:
        error_msg = str(e)
        # If folder already exists (409), try to find it by name
        if "409" in error_msg or "already" in error_msg.lower():
            folders = discogs_get_collection_folders_with_names(username)
            return folders.get(folder_name)
        print(f"Warning: Failed to create folder '{folder_name}': {e}")
        return None

def discogs_get_or_create_folder(username: str, folder_name: str):
    """
    Get folder_id for a folder by name, creating it if it doesn't exist.
    Returns folder_id or None on error.
    """
    # Check if folder already exists
    folders = discogs_get_collection_folders_with_names(username)
    if folder_name in folders:
        return folders[folder_name]
    
    # Create the folder
    print(f"Creating folder: {folder_name}")
    folder_id = discogs_create_folder(username, folder_name)
    if folder_id:
        # Clear cache so next call gets updated folder list
        discogs_get_collection_folders_with_names.cache_clear()
        discogs_get_collection_folders.cache_clear()
    return folder_id

def discogs_move_instance(username: str, release_id: int, instance_id: int, 
                          source_folder_id: int, target_folder_id: int):
    """
    Move an instance from one folder to another.
    Uses POST to the CURRENT (source) folder endpoint with folder_id in the JSON body.
    According to Discogs API: POST to current folder, send {"folder_id": target_folder_id} in body.
    """
    if source_folder_id == target_folder_id:
        return True  # Already in the right folder
    
    # POST to the CURRENT folder endpoint (not the target folder!)
    url = f"https://api.discogs.com/users/{username}/collection/folders/{source_folder_id}/releases/{release_id}/instances/{instance_id}"
    headers = discogs_headers()
    headers["Content-Type"] = "application/json"
    
    try:
        # Send folder_id in JSON body to move to target folder
        http_post_with_retry(url, headers=headers, json_data={"folder_id": target_folder_id}, timeout=20)
        return True
    except Exception as e:
        error_msg = str(e)
        # 409 might mean it's already there, which is fine
        if "409" in error_msg or "already" in error_msg.lower():
            return True
        print(f"Warning: Failed to move instance {instance_id} (release {release_id}) from folder {source_folder_id} to folder {target_folder_id}: {e}")
        return False

def discogs_list_folder_release_ids(username: str, folder_id: int):
    """Return a set of release IDs present in a specific folder."""
    ids = set()
    params = {"per_page": 100, "page": 1}
    while True:
        r = http_get_with_retry(
            f"https://api.discogs.com/users/{username}/collection/folders/{folder_id}/releases",
            headers=discogs_headers(), params=params, timeout=30
        )
        js = r.json()
        for item in js.get("releases", []):
            bi = item.get("basic_information", {})
            rid = bi.get("id")
            if rid:
                ids.add(int(rid))
        pg = js.get("pagination", {})
        if pg.get("page", 1) < pg.get("pages", 1):
            params["page"] = pg["page"] + 1
            continue
        break
    return ids

def discogs_list_folder_releases(username: str, folder_id: int):
    """
    List all releases in a folder with full metadata (title, artist, year, etc.).
    Returns a list of dicts with: release_id, album_title, artist_name, year, discogs_url
    """
    releases = []
    params = {"per_page": 100, "page": 1}
    while True:
        r = http_get_with_retry(
            f"https://api.discogs.com/users/{username}/collection/folders/{folder_id}/releases",
            headers=discogs_headers(), params=params, timeout=30
        )
        js = r.json()
        for item in js.get("releases", []):
            bi = item.get("basic_information", {})
            release_id = bi.get("id")
            if not release_id:
                continue
            
            album_title = bi.get("title", "")
            artists = bi.get("artists", [])
            artist_name = artists[0].get("name", "") if artists else ""
            year = bi.get("year", 0)
            resource_url = bi.get("resource_url", "")
            discogs_url = resource_url or f"https://www.discogs.com/release/{release_id}"
            
            releases.append({
                "release_id": int(release_id),
                "album_title": album_title,
                "artist_name": artist_name,
                "year": int(year) if year else None,
                "discogs_url": discogs_url
            })
        
        pg = js.get("pagination", {})
        if pg.get("page", 1) < pg.get("pages", 1):
            params["page"] = pg["page"] + 1
            time.sleep(0.5)  # Rate limiting
            continue
        break
    return releases

def discogs_get_release_tracklist(release_id: int):
    """
    Fetch tracklist from Discogs release endpoint.
    Returns a list of dicts with: position, title, duration (if available)
    """
    try:
        release_data = discogs_get_release(release_id)
        if not release_data:
            return []
        
        tracklist = release_data.get("tracklist", [])
        tracks = []
        for track in tracklist:
            tracks.append({
                "position": track.get("position", ""),
                "title": track.get("title", ""),
                "duration": track.get("duration", "")
            })
        return tracks
    except Exception as e:
        print(f"Failed to fetch tracklist for release {release_id}: {e}")
        return []

def discogs_list_all_collection_release_ids(username: str):
    """Return a set of ALL release IDs in the user's collection (across all folders)."""
    all_ids = set()
    for fid in discogs_get_collection_folders(username):
        all_ids |= discogs_list_folder_release_ids(username, fid)
    return all_ids

def discogs_get_instance_for_release(username: str, release_id: int, folder_id: int = 1):
    """
    Find instance_id for a given release_id by listing collection items in a specific folder.
    Returns tuple (instance_id, actual_folder_id) if found, (None, None) otherwise.
    Default folder_id=1 (Uncategorized) since new releases are added there by default.
    """
    params = {"per_page": 100, "page": 1}
    while True:
        r = http_get_with_retry(
            f"https://api.discogs.com/users/{username}/collection/folders/{folder_id}/releases",
            headers=discogs_headers(), params=params, timeout=30
        )
        js = r.json()
        for item in js.get("releases", []):
            bi = item.get("basic_information", {})
            item_release_id = bi.get("id")
            if item_release_id == release_id:
                # Found it - get instance_id and use folder_id from item if available, otherwise use folder_id parameter
                instance_id = item.get("instance_id") or item.get("id")
                actual_folder_id = item.get("folder_id") or folder_id
                return (instance_id, actual_folder_id)
        pg = js.get("pagination", {})
        if pg.get("page", 1) < pg.get("pages", 1):
            params["page"] = pg["page"] + 1
            time.sleep(0.5)
            continue
        break
    return (None, None)

def discogs_get_instance_conditions(username: str, folder_id: int, release_id: int, instance_id: int):
    """
    Get current media and sleeve condition values for an instance.
    Returns dict with 'media_condition' and 'sleeve_condition' (None if not set).
    """
    # Get field IDs
    field_ids = discogs_get_collection_field_ids(username)
    media_field_id = field_ids.get("media_condition")
    sleeve_field_id = field_ids.get("sleeve_condition")
    
    if not media_field_id or not sleeve_field_id:
        return {"media_condition": None, "sleeve_condition": None}
    
    # Fetch instance details
    url = f"https://api.discogs.com/users/{username}/collection/folders/{folder_id}/releases/{release_id}/instances/{instance_id}"
    try:
        r = http_get_with_retry(url, headers=discogs_headers(), timeout=20)
        instance_data = r.json()
        
        # Read conditions from notes/fields array
        notes = instance_data.get("notes", [])
        media_condition = None
        sleeve_condition = None
        
        for note in notes:
            note_field_id = note.get("field_id")
            note_value = note.get("value")
            if note_field_id == media_field_id:
                media_condition = note_value
            elif note_field_id == sleeve_field_id:
                sleeve_condition = note_value
        
        return {
            "media_condition": media_condition,
            "sleeve_condition": sleeve_condition
        }
    except Exception as e:
        # If we can't fetch, assume no conditions set
        return {"media_condition": None, "sleeve_condition": None}

def discogs_list_all_collection_instances(username: str):
    """
    List ALL collection instances by iterating through each folder.
    Returns a list of dicts with: release_id, instance_id, folder_id, media_condition, sleeve_condition
    Note: Listing from folder 0 may not provide correct instance_id, so we list each folder separately.
    """
    instances = []
    
    # Get field IDs for condition fields
    field_ids = discogs_get_collection_field_ids(username)
    media_field_id = field_ids.get("media_condition")
    sleeve_field_id = field_ids.get("sleeve_condition")
    
    if not media_field_id or not sleeve_field_id:
        print("ERROR: Could not find Media Condition or Sleeve Condition field IDs. Skipping condition updates.")
        print(f"Available fields: {[(f.get('id'), f.get('name')) for f in discogs_get_collection_field_ids.__wrapped__(username).get('fields', [])]}")
        return instances
    
    print(f"Using field IDs - Media: {media_field_id}, Sleeve: {sleeve_field_id}")
    
    # Get all folders and iterate through each one
    folders = discogs_get_collection_folders(username)
    print(f"Found {len(folders)} folders to check")
    
    for folder_id in folders:
        params = {"per_page": 100, "page": 1}
        while True:
            r = http_get_with_retry(
                f"https://api.discogs.com/users/{username}/collection/folders/{folder_id}/releases",
                headers=discogs_headers(), params=params, timeout=30
            )
            js = r.json()
            for item in js.get("releases", []):
                bi = item.get("basic_information", {})
                release_id = bi.get("id")  # Release ID from basic_information
                instance_id = item.get("instance_id")  # Instance ID (unique key for this copy in collection)
                folder_id_from_item = item.get("folder_id")  # Folder where this instance currently resides
                
                # Use folder_id from item if available, otherwise use the folder we're iterating through
                actual_folder_id = folder_id_from_item if folder_id_from_item else folder_id
                
                # Skip if we don't have the essential IDs
                if not release_id or not instance_id:
                    continue
                
                # Validate instance_id != release_id (they should be different)
                if instance_id == release_id:
                    # This shouldn't happen, but log and skip
                    if len(instances) == 0:  # Only print first occurrence
                        import json
                        print(f"DEBUG: Found item where instance_id == release_id ({release_id}) in folder {folder_id}")
                        print(f"DEBUG: Item keys: {list(item.keys())}")
                        print(f"DEBUG: Full item JSON:\n{json.dumps(item, indent=2)}")
                    continue
                
                # Read conditions from notes/fields array
                notes = item.get("notes", [])
                media_condition = None
                sleeve_condition = None
                
                for note in notes:
                    note_field_id = note.get("field_id")
                    note_value = note.get("value")
                    if note_field_id == media_field_id:
                        media_condition = note_value
                    elif note_field_id == sleeve_field_id:
                        sleeve_condition = note_value
                
                instances.append({
                    "release_id": int(release_id),
                    "instance_id": int(instance_id),
                    "folder_id": int(actual_folder_id),  # Use folder_id from item, or folder we're iterating through
                    "media_condition": media_condition,
                    "sleeve_condition": sleeve_condition
                })
            
            pg = js.get("pagination", {})
            if pg.get("page", 1) < pg.get("pages", 1):
                params["page"] = pg["page"] + 1
                time.sleep(0.5)  # Small delay to avoid rate limiting
                continue
            break
    
    return instances

def discogs_update_instance_condition(username: str, folder_id: int, release_id: int, instance_id: int,
                                      media_condition=None, sleeve_condition=None):
    """
    Update the condition ratings for a specific collection instance.
    Uses the correct Discogs API endpoint: POST /instances/{instance_id}/fields/{field_id}
    Follows protocol: folder_id in path must be the folder where instance currently resides.
    """
    # Get field IDs
    field_ids = discogs_get_collection_field_ids(username)
    media_field_id = field_ids.get("media_condition")
    sleeve_field_id = field_ids.get("sleeve_condition")
    
    if not media_field_id or not sleeve_field_id:
        raise Exception(f"Could not find Media Condition or Sleeve Condition field IDs. "
                       f"Found: {field_ids}. Check collection fields.")
    
    headers = discogs_headers()
    headers["Content-Type"] = "application/json"
    
    # Update Media Condition (only if provided)
    if media_condition:
        url_media = f"https://api.discogs.com/users/{username}/collection/folders/{folder_id}/releases/{release_id}/instances/{instance_id}/fields/{media_field_id}"
        http_post_with_retry(url_media, headers=headers, json_data={"value": media_condition}, timeout=20)
        time.sleep(0.6)  # Rate limiting between field updates
    
    # Update Sleeve Condition (only if provided)
    if sleeve_condition:
        url_sleeve = f"https://api.discogs.com/users/{username}/collection/folders/{folder_id}/releases/{release_id}/instances/{instance_id}/fields/{sleeve_field_id}"
        http_post_with_retry(url_sleeve, headers=headers, json_data={"value": sleeve_condition}, timeout=20)
    
    return True

# Micro-caches to reduce duplicate calls
@lru_cache(maxsize=4096)
def cached_release_from_master(master_id: int):
    return discogs_release_from_master(master_id, context=None)

def cached_discogs_search(artist, title, context=None):
    # Note: Can't use lru_cache with context parameter, so we'll call discogs_search directly
    # Returns list of results, not single result
    return discogs_search(artist=artist, title=title, context=context)


# ----------------- Spotify API -----------------

def clean_artist_name_for_spotify(artist_name: str):
    """
    Strip Discogs parenthetical numbering from artist names.
    Example: "Kansas (2)" -> "Kansas"
    This is needed because Spotify doesn't use this convention.
    """
    if not artist_name:
        return artist_name
    
    # Match pattern like "Artist Name (2)" or "Artist Name (123)"
    # This regex matches: optional whitespace, opening paren, one or more digits, closing paren, end of string
    cleaned = re.sub(r'\s*\(\d+\)\s*$', '', artist_name).strip()
    return cleaned

def spotify_authenticate():
    """
    Authenticate with Spotify using spotipy OAuth flow.
    Returns spotipy.Spotify client or None on error.
    """
    if not SPOTIPY_AVAILABLE:
        raise SystemExit("spotipy is not installed. Install it with: pip install spotipy")
    
    client_id = os.environ.get("SPOTIPY_CLIENT_ID")
    client_secret = os.environ.get("SPOTIPY_CLIENT_SECRET")
    redirect_uri = os.environ.get("SPOTIPY_REDIRECT_URI")
    
    if not all([client_id, client_secret, redirect_uri]):
        raise SystemExit(
            "Spotify credentials not set. Required environment variables:\n"
            "  SPOTIPY_CLIENT_ID\n"
            "  SPOTIPY_CLIENT_SECRET\n"
            "  SPOTIPY_REDIRECT_URI\n"
            "Set these or unset DISCOGS_PLAYLIST_SOURCE_FOLDER to skip playlist building."
        )
    
    try:
        scope = "playlist-modify-private playlist-modify-public"
        auth_manager = SpotifyOAuth(
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri=redirect_uri,
            scope=scope
        )
        sp = spotipy.Spotify(auth_manager=auth_manager)
        # Test authentication
        sp.current_user()
        return sp
    except Exception as e:
        raise SystemExit(f"Spotify authentication failed: {e}\nCheck your credentials and redirect URI.")

def spotify_search_album(album_title: str, artist_name: str, discogs_year: int = None, sp=None):
    """
    Search Spotify for album with matching heuristics.
    Returns (album_id, album_data) or (None, None) if not found.
    """
    if not sp:
        return None, None
    
    # Clean artist name to remove Discogs parenthetical numbering
    cleaned_artist = clean_artist_name_for_spotify(artist_name)
    
    # Build query
    query = f'album:"{album_title}" artist:"{cleaned_artist}"'
    
    try:
        results = sp.search(q=query, type="album", limit=20)
        albums = results.get("albums", {}).get("items", [])
        
        if not albums:
            return None, None
        
        # Heuristic 1: Exact (case-insensitive) match on album title and artist
        exact_matches = []
        cleaned_artist_lower = cleaned_artist.lower()
        for album in albums:
            album_name = album.get("name", "").lower()
            album_artists = [a.get("name", "").lower() for a in album.get("artists", [])]
            
            if album_name == album_title.lower() and cleaned_artist_lower in album_artists:
                exact_matches.append(album)
        
        if len(exact_matches) == 1:
            return exact_matches[0].get("id"), exact_matches[0]
        
        if len(exact_matches) > 1 and discogs_year:
            # Heuristic 2: Prefer release year closest to Discogs year (±2 years)
            best_match = None
            best_diff = float('inf')
            for album in exact_matches:
                release_date = album.get("release_date", "")
                if release_date:
                    try:
                        album_year = int(release_date.split("-")[0])
                        diff = abs(album_year - discogs_year)
                        if diff <= 2 and diff < best_diff:
                            best_diff = diff
                            best_match = album
                    except (ValueError, IndexError):
                        pass
            
            if best_match:
                return best_match.get("id"), best_match
        
        # Heuristic 3: Prefer canonical/non-deluxe unless Discogs title clearly indicates deluxe
        if exact_matches:
            # Check if Discogs title has deluxe/special indicators
            discogs_lower = album_title.lower()
            has_deluxe_keywords = any(kw in discogs_lower for kw in ["deluxe", "special", "expanded", "remastered"])
            
            for album in exact_matches:
                album_name_lower = album.get("name", "").lower()
                # If Discogs doesn't have deluxe keywords, prefer non-deluxe
                if not has_deluxe_keywords:
                    if not any(kw in album_name_lower for kw in ["deluxe", "special edition", "expanded"]):
                        return album.get("id"), album
                # If Discogs has deluxe keywords, prefer matching deluxe
                else:
                    if any(kw in album_name_lower for kw in ["deluxe", "special", "expanded"]):
                        return album.get("id"), album
            
            # Fallback: return first exact match
            return exact_matches[0].get("id"), exact_matches[0]
        
        # No exact match, return first result
        return albums[0].get("id"), albums[0]
    
    except Exception as e:
        print(f"Spotify search error for album '{album_title}' by '{artist_name}': {e}")
        return None, None

def spotify_search_track(track_title: str, artist_name: str, album_title: str = None, sp=None):
    """
    Search Spotify for track with fallback queries.
    Returns (track_uri, track_data) or (None, None) if not found.
    """
    if not sp:
        return None, None
    
    # Clean artist name to remove Discogs parenthetical numbering
    cleaned_artist = clean_artist_name_for_spotify(artist_name)
    
    # Try with album first
    if album_title:
        query = f'track:"{track_title}" artist:"{cleaned_artist}" album:"{album_title}"'
        try:
            results = sp.search(q=query, type="track", limit=5)
            tracks = results.get("tracks", {}).get("items", [])
            if tracks:
                return tracks[0].get("uri"), tracks[0]
        except Exception:
            pass
    
    # Fallback: just track + artist
    query = f'track:"{track_title}" artist:"{cleaned_artist}"'
    try:
        results = sp.search(q=query, type="track", limit=5)
        tracks = results.get("tracks", {}).get("items", [])
        if tracks:
            return tracks[0].get("uri"), tracks[0]
    except Exception as e:
        print(f"Spotify track search error for '{track_title}' by '{artist_name}': {e}")
    
    return None, None

def spotify_get_album_tracks(album_id: str, sp=None):
    """
    Fetch all tracks from Spotify album (handle multi-disc).
    Returns list of track URIs in order.
    """
    if not sp or not album_id:
        return []
    
    try:
        tracks = []
        results = sp.album_tracks(album_id, limit=50)
        
        while results:
            for item in results.get("items", []):
                track_uri = item.get("uri")
                if track_uri:
                    tracks.append(track_uri)
            
            if results.get("next"):
                results = sp.next(results)
            else:
                break
        
        return tracks
    except Exception as e:
        print(f"Failed to fetch tracks for album {album_id}: {e}")
        return []

def spotify_extract_playlist_id(url: str) -> str:
    """
    Extract playlist ID from Spotify URL or URI.
    Supports formats:
    - https://open.spotify.com/playlist/{id}
    - spotify:playlist:{id}
    - Direct playlist ID (for convenience)
    Returns playlist ID or None if invalid.
    """
    if not url:
        return None
    
    url = url.strip()
    
    # Direct playlist ID (alphanumeric, 22 chars typically)
    if len(url) == 22 and url.replace('-', '').replace('_', '').isalnum():
        return url
    
    # Handle spotify:playlist:{id} format
    if url.startswith("spotify:playlist:"):
        playlist_id = url.replace("spotify:playlist:", "").split("?")[0]
        return playlist_id if playlist_id else None
    
    # Handle https://open.spotify.com/playlist/{id} format
    if "open.spotify.com/playlist/" in url:
        try:
            # Extract ID from URL (may have query params or track info)
            parts = url.split("/playlist/")
            if len(parts) > 1:
                playlist_id = parts[1].split("?")[0].split("&")[0]
                return playlist_id if playlist_id else None
        except Exception:
            pass
    
    return None

def get_folders_from_gcs_prefix(prefix: str):
    """
    Extract folder names that would be created from GCS paths under the given prefix.
    Returns a set of folder names (e.g., {"Dad", "Dad_Shed"}).
    """
    if not prefix or not GCS_BUCKET:
        return set()
    
    try:
        gcs = storage.Client()
        bucket = gcs.bucket(GCS_BUCKET)
        # List all blobs under the prefix
        blobs = bucket.list_blobs(prefix=prefix)
        
        folder_names = set()
        for blob in blobs:
            if blob.name.lower().endswith((".jpg", ".jpeg", ".png")):
                # Extract folder name using the same logic as owner_from_gcs_uri
                uri = gcs_uri(blob.name)
                folder_name = owner_from_gcs_uri(uri)
                if folder_name:
                    folder_names.add(folder_name)
        
        return folder_names
    except Exception as e:
        print(f"Warning: Could not list GCS blobs under prefix '{prefix}': {e}")
        return set()

def spotify_get_playlist_tracks(playlist_id: str, sp=None):
    """
    Fetch all existing track URIs from a Spotify playlist.
    Returns a set of track URIs for efficient lookup.
    Handles pagination similar to spotify_get_album_tracks.
    """
    if not sp or not playlist_id:
        return set()
    
    try:
        track_uris = set()
        results = sp.playlist_tracks(playlist_id, limit=100)
        
        while results:
            for item in results.get("items", []):
                track = item.get("track")
                if track:
                    track_uri = track.get("uri")
                    if track_uri:
                        track_uris.add(track_uri)
            
            if results.get("next"):
                results = sp.next(results)
            else:
                break
        
        return track_uris
    except Exception as e:
        print(f"Failed to fetch tracks from playlist {playlist_id}: {e}")
        return set()

def spotify_create_playlist(name: str, description: str, public: bool = False, sp=None):
    """
    Create Spotify playlist and return playlist ID and URL.
    Returns (playlist_id, playlist_url) or (None, None) on error.
    """
    if not sp:
        return None, None
    
    try:
        user_id = sp.current_user()["id"]
        playlist = sp.user_playlist_create(
            user=user_id,
            name=name,
            public=public,
            description=description
        )
        playlist_id = playlist.get("id")
        playlist_url = playlist.get("external_urls", {}).get("spotify", "")
        return playlist_id, playlist_url
    except Exception as e:
        print(f"Failed to create playlist '{name}': {e}")
        return None, None

def spotify_add_tracks_to_playlist(playlist_id: str, track_uris: list, sp=None):
    """
    Add tracks to playlist in batches of 100.
    Returns number of tracks successfully added.
    """
    if not sp or not playlist_id or not track_uris:
        return 0
    
    added = 0
    batch_size = 100
    
    for i in range(0, len(track_uris), batch_size):
        batch = track_uris[i:i + batch_size]
        try:
            sp.playlist_add_items(playlist_id, batch)
            added += len(batch)
            time.sleep(0.2)  # Rate limiting
        except Exception as e:
            print(f"Failed to add batch to playlist {playlist_id}: {e}")
    
    return added

def build_spotify_playlists():
    """
    Main orchestration function for building Spotify playlists from Discogs collection folders.
    """
    # Review gate
    print("\n" + "="*80)
    print("Discogs is now the source of truth.")
    response = input("Do you want to proceed to build Spotify playlists?\nPress Enter to continue, or type 'skip' to stop here: ").strip()
    
    if response.lower() == "skip":
        print("Skipping Spotify playlist building.")
        return
    
    # Check if Spotify credentials are available
    if not all([os.environ.get("SPOTIPY_CLIENT_ID"), 
                os.environ.get("SPOTIPY_CLIENT_SECRET"), 
                os.environ.get("SPOTIPY_REDIRECT_URI")]):
        print("Spotify credentials not set. Skipping playlist building.")
        print("Set SPOTIPY_CLIENT_ID, SPOTIPY_CLIENT_SECRET, and SPOTIPY_REDIRECT_URI to enable playlist building.")
        return
    
    # Authenticate with Spotify
    print("\nAuthenticating with Spotify...")
    try:
        sp = spotify_authenticate()
        print("Spotify authentication successful.")
    except SystemExit as e:
        print(f"Spotify authentication failed: {e}")
        return
    except Exception as e:
        print(f"Unexpected error during Spotify authentication: {e}")
        return
    
    # Check if existing playlist URL is provided
    if SPOTIFY_PLAYLIST_URL:
        # Use existing playlist mode - skip folder-based creation
        playlist_id = spotify_extract_playlist_id(SPOTIFY_PLAYLIST_URL)
        if not playlist_id:
            print(f"Error: Invalid Spotify playlist URL: {SPOTIFY_PLAYLIST_URL}")
            print("Supported formats:")
            print("  - https://open.spotify.com/playlist/{id}")
            print("  - spotify:playlist:{id}")
            print("  - Direct playlist ID")
            return
        
        print(f"\nUsing existing playlist: {SPOTIFY_PLAYLIST_URL}")
        print(f"Extracted playlist ID: {playlist_id}")
        
        # Fetch existing tracks from playlist
        print("Fetching existing tracks from playlist...")
        existing_playlist_tracks = spotify_get_playlist_tracks(playlist_id, sp=sp)
        print(f"Found {len(existing_playlist_tracks)} existing tracks in playlist")
        
        # Determine folders to process
        if not DISCOGS_USER or not DISCOGS_TOKEN:
            print("DISCOGS_USER or DISCOGS_TOKEN not set. Cannot fetch collection folders.")
            return
        
        folders_dict = discogs_get_collection_folders_with_names(DISCOGS_USER)
        folders_to_process = []
        
        # Check if INPUT_PREFIX was customized (different from default)
        default_prefix = os.getenv("VINYL_INPUT_PREFIX", "covers/").strip()
        prefix_was_customized = INPUT_PREFIX != default_prefix
        
        # If INPUT_PREFIX was customized, filter folders based on GCS structure
        gcs_folder_names = set()
        if prefix_was_customized:
            print(f"\nINPUT_PREFIX was customized to: {INPUT_PREFIX}")
            print("Extracting folder names from GCS paths...")
            gcs_folder_names = get_folders_from_gcs_prefix(INPUT_PREFIX)
            if gcs_folder_names:
                print(f"Found folders in GCS: {', '.join(sorted(gcs_folder_names))}")
            else:
                print("Warning: No folders found in GCS under the specified prefix.")
        
        # If INPUT_PREFIX was customized via --input-prefix, it takes precedence over DISCOGS_PLAYLIST_SOURCE_FOLDER
        if prefix_was_customized:
            # Process only folders found in GCS under the specified prefix (ignore DISCOGS_PLAYLIST_SOURCE_FOLDER)
            if gcs_folder_names:
                print(f"\n--input-prefix takes precedence. Processing folders found in GCS: {', '.join(sorted(gcs_folder_names))}")
                for name, folder_id in folders_dict.items():
                    if name in gcs_folder_names:
                        folders_to_process.append((folder_id, name))
            else:
                print(f"Warning: No folders found in GCS under prefix '{INPUT_PREFIX}'. Nothing to process.")
                return
        elif DISCOGS_PLAYLIST_SOURCE_FOLDER:
            # Single folder mode (only when --input-prefix is NOT set)
            folder_name_lower = DISCOGS_PLAYLIST_SOURCE_FOLDER.lower()
            folder_id = None
            for name, fid in folders_dict.items():
                if name.lower() == folder_name_lower:
                    folder_id = fid
                    folders_to_process.append((fid, name))
                    break
            
            if not folder_id:
                print(f"Error: Folder '{DISCOGS_PLAYLIST_SOURCE_FOLDER}' not found in your Discogs collection.")
                print(f"Available folders: {', '.join(folders_dict.keys())}")
                return
        else:
            # Multi-folder mode: process all custom folders (IDs >= 2)
            for name, folder_id in folders_dict.items():
                if folder_id >= 2:  # Skip system folders (0 = All, 1 = Uncategorized)
                    folders_to_process.append((folder_id, name))
        
        if not folders_to_process:
            print("No folders to process.")
            return
        
        # Print which folders will be processed
        folder_names = [name for _, name in folders_to_process]
        if prefix_was_customized:
            print(f"\nProcessing {len(folders_to_process)} folder(s) from GCS prefix '{INPUT_PREFIX}' and adding tracks to existing playlist...")
            print(f"Folders: {', '.join(folder_names)}")
            if DISCOGS_PLAYLIST_SOURCE_FOLDER:
                print(f"Note: --input-prefix takes precedence over DISCOGS_PLAYLIST_SOURCE_FOLDER='{DISCOGS_PLAYLIST_SOURCE_FOLDER}'")
        elif DISCOGS_PLAYLIST_SOURCE_FOLDER:
            print(f"\nProcessing folder '{DISCOGS_PLAYLIST_SOURCE_FOLDER}' and adding tracks to existing playlist...")
        else:
            print(f"\nProcessing {len(folders_to_process)} folder(s) and adding tracks to existing playlist...")
            print(f"Folders: {', '.join(folder_names)}")
        print("Only tracks that don't already exist in the playlist will be added.")
        
        # Track unmatched albums and tracks for CSV output
        unmatched_albums = []
        unmatched_tracks = []
        all_new_tracks = []  # Collect all tracks from all folders
        seen_albums = set()  # De-duplication across all folders
        
        # Process each folder and collect tracks
        for folder_id, folder_name in folders_to_process:
            print(f"\n{'='*80}")
            print(f"Processing folder: {folder_name} (ID: {folder_id})")
            print(f"{'='*80}")
            
            # Fetch releases from folder
            print(f"Fetching releases from folder '{folder_name}'...")
            releases = discogs_list_folder_releases(DISCOGS_USER, folder_id)
            print(f"Found {len(releases)} releases in folder '{folder_name}'")
            
            if not releases:
                continue
            
            # Track statistics for this folder
            album_matches = 0
            partial_matches = 0
            unmatched_count = 0
            
            # Process each release
            for idx, release in enumerate(releases, 1):
                release_id = release["release_id"]
                album_title = release["album_title"]
                artist_name = release["artist_name"]
                year = release.get("year")
                discogs_url = release["discogs_url"]
                
                print(f"\n[{idx}/{len(releases)}] {artist_name} - {album_title}")
                
                # Skip if we've already processed this album (de-duplication)
                album_key = (album_title.lower(), artist_name.lower())
                if album_key in seen_albums:
                    print(f"  Skipping duplicate album")
                    continue
                seen_albums.add(album_key)
                
                # Try album-level match
                album_id, album_data = spotify_search_album(album_title, artist_name, year, sp=sp)
                
                if album_id:
                    # Album matched - get all tracks
                    print(f"  ✓ Album matched on Spotify")
                    album_tracks = spotify_get_album_tracks(album_id, sp=sp)
                    
                    if album_tracks:
                        all_new_tracks.extend(album_tracks)
                        album_matches += 1
                        print(f"  Found {len(album_tracks)} tracks from album")
                    else:
                        print(f"  Warning: Album matched but no tracks found")
                        unmatched_count += 1
                        unmatched_albums.append({
                            "folder_name": folder_name,
                            "discogs_release_id": release_id,
                            "discogs_url": discogs_url,
                            "album_title": album_title,
                            "artist_name": artist_name,
                            "notes": "Album matched but no tracks available"
                        })
                else:
                    # Album not matched - try track-level fallback
                    print(f"  Album not found, trying track-level matching...")
                    tracklist = discogs_get_release_tracklist(release_id)
                    
                    if not tracklist:
                        print(f"  No tracklist available on Discogs")
                        unmatched_count += 1
                        unmatched_albums.append({
                            "folder_name": folder_name,
                            "discogs_release_id": release_id,
                            "discogs_url": discogs_url,
                            "album_title": album_title,
                            "artist_name": artist_name,
                            "notes": "Album not found, no tracklist available"
                        })
                        continue
                    
                    matched_tracks = []
                    for track in tracklist:
                        track_title = track.get("title", "").strip()
                        if not track_title:
                            continue
                        
                        track_uri, _ = spotify_search_track(track_title, artist_name, album_title, sp=sp)
                        if track_uri:
                            matched_tracks.append(track_uri)
                        else:
                            # Track not matched - add to unmatched tracks list
                            unmatched_tracks.append({
                                "folder_name": folder_name,
                                "discogs_release_id": release_id,
                                "discogs_url": discogs_url,
                                "album_title": album_title,
                                "artist_name": artist_name,
                                "track_title": track_title,
                                "track_position": track.get("position", ""),
                                "notes": "Track not found on Spotify"
                            })
                    
                    if matched_tracks:
                        all_new_tracks.extend(matched_tracks)
                        partial_matches += 1
                        print(f"  ✓ Matched {len(matched_tracks)}/{len(tracklist)} tracks")
                    else:
                        unmatched_count += 1
                        unmatched_albums.append({
                            "folder_name": folder_name,
                            "discogs_release_id": release_id,
                            "discogs_url": discogs_url,
                            "album_title": album_title,
                            "artist_name": artist_name,
                            "notes": "Album not found, no tracks matched"
                        })
                        print(f"  ✗ No tracks matched")
                
                time.sleep(0.3)  # Rate limiting between releases
            
            # Summary for this folder
            print(f"\n{'='*80}")
            print(f"Folder '{folder_name}' Summary:")
            print(f"  Albums fully matched (album-level): {album_matches}")
            print(f"  Albums partially matched (track-level): {partial_matches}")
            print(f"  Albums unmatched: {unmatched_count}")
            print(f"{'='*80}\n")
        
        # Filter out tracks that already exist in the playlist
        print(f"\nFiltering tracks...")
        print(f"  Total tracks found: {len(all_new_tracks)}")
        new_tracks = [uri for uri in all_new_tracks if uri not in existing_playlist_tracks]
        skipped_count = len(all_new_tracks) - len(new_tracks)
        print(f"  New tracks to add: {len(new_tracks)}")
        print(f"  Tracks already in playlist (skipped): {skipped_count}")
        
        # Add new tracks to existing playlist
        if new_tracks:
            print(f"\nAdding {len(new_tracks)} new tracks to existing playlist...")
            added_count = spotify_add_tracks_to_playlist(playlist_id, new_tracks, sp=sp)
            print(f"Successfully added {added_count} tracks to playlist")
            print(f"Playlist URL: https://open.spotify.com/playlist/{playlist_id}")
        else:
            print(f"\nNo new tracks to add - all tracks already exist in the playlist.")
        
        # Write unmatched CSVs
        if unmatched_albums:
            unmatched_df = pd.DataFrame(unmatched_albums)
            unmatched_csv = "unmatched_albums.csv"
            unmatched_df.to_csv(unmatched_csv, index=False)
            print(f"\nWrote {len(unmatched_albums)} unmatched albums to {unmatched_csv}")
            print("You can manually review and add these to Spotify later.")
        else:
            print("\nAll albums were matched successfully!")
        
        if unmatched_tracks:
            unmatched_tracks_df = pd.DataFrame(unmatched_tracks)
            unmatched_tracks_csv = "unmatched_tracks.csv"
            unmatched_tracks_df.to_csv(unmatched_tracks_csv, index=False)
            print(f"\nWrote {len(unmatched_tracks)} unmatched tracks to {unmatched_tracks_csv}")
            print("You can manually review and add these to Spotify later.")
        else:
            print("\nAll tracks were matched successfully!")
        
        return
    
    # Original folder-based playlist creation mode (when SPOTIFY_PLAYLIST_URL is not set)
    # Determine folders to process
    if not DISCOGS_USER or not DISCOGS_TOKEN:
        print("DISCOGS_USER or DISCOGS_TOKEN not set. Cannot fetch collection folders.")
        return
    
    folders_dict = discogs_get_collection_folders_with_names(DISCOGS_USER)
    folders_to_process = []
    
    # Check if INPUT_PREFIX was customized (different from default)
    default_prefix = os.getenv("VINYL_INPUT_PREFIX", "covers/").strip()
    prefix_was_customized = INPUT_PREFIX != default_prefix
    
    # If INPUT_PREFIX was customized, filter folders based on GCS structure
    gcs_folder_names = set()
    if prefix_was_customized:
        print(f"\nINPUT_PREFIX was customized to: {INPUT_PREFIX}")
        print("Extracting folder names from GCS paths...")
        gcs_folder_names = get_folders_from_gcs_prefix(INPUT_PREFIX)
        if gcs_folder_names:
            print(f"Found folders in GCS: {', '.join(sorted(gcs_folder_names))}")
        else:
            print("Warning: No folders found in GCS under the specified prefix.")
    
    # If INPUT_PREFIX was customized via --input-prefix, it takes precedence over DISCOGS_PLAYLIST_SOURCE_FOLDER
    if prefix_was_customized:
        # Process only folders found in GCS under the specified prefix (ignore DISCOGS_PLAYLIST_SOURCE_FOLDER)
        if gcs_folder_names:
            print(f"\n--input-prefix takes precedence. Processing folders found in GCS: {', '.join(sorted(gcs_folder_names))}")
            for name, folder_id in folders_dict.items():
                if name in gcs_folder_names:
                    folders_to_process.append((folder_id, name))
        else:
            print(f"Warning: No folders found in GCS under prefix '{INPUT_PREFIX}'. Nothing to process.")
            return
    elif DISCOGS_PLAYLIST_SOURCE_FOLDER:
        # Single folder mode (only when --input-prefix is NOT set)
        folder_name_lower = DISCOGS_PLAYLIST_SOURCE_FOLDER.lower()
        folder_id = None
        for name, fid in folders_dict.items():
            if name.lower() == folder_name_lower:
                folder_id = fid
                folders_to_process.append((fid, name))
                break
        
        if not folder_id:
            print(f"Error: Folder '{DISCOGS_PLAYLIST_SOURCE_FOLDER}' not found in your Discogs collection.")
            print(f"Available folders: {', '.join(folders_dict.keys())}")
            return
    else:
        # Multi-folder mode: process all custom folders (IDs >= 2)
        for name, folder_id in folders_dict.items():
            if folder_id >= 2:  # Skip system folders (0 = All, 1 = Uncategorized)
                folders_to_process.append((folder_id, name))
    
    if not folders_to_process:
        print("No folders to process.")
        return
    
    print(f"\nProcessing {len(folders_to_process)} folder(s)...")
    
    # Track unmatched albums and tracks for CSV output
    unmatched_albums = []
    unmatched_tracks = []
    all_track_uris = set()  # For de-duplication across all playlists
    
    # Process each folder
    for folder_id, folder_name in folders_to_process:
        print(f"\n{'='*80}")
        print(f"Processing folder: {folder_name} (ID: {folder_id})")
        print(f"{'='*80}")
        
        # Fetch releases from folder
        print(f"Fetching releases from folder '{folder_name}'...")
        releases = discogs_list_folder_releases(DISCOGS_USER, folder_id)
        print(f"Found {len(releases)} releases in folder '{folder_name}'")
        
        if not releases:
            continue
        
        # Create playlist
        today = datetime.now().strftime("%Y-%m-%d")
        playlist_name = f"{folder_name} — Discogs albums ({today})"
        playlist_description = f"Built from Discogs folder: {folder_name}"
        
        print(f"Creating playlist: {playlist_name}")
        playlist_id, playlist_url = spotify_create_playlist(playlist_name, playlist_description, public=False, sp=sp)
        
        if not playlist_id:
            print(f"Failed to create playlist for folder '{folder_name}'. Skipping.")
            continue
        
        print(f"Playlist created: {playlist_url}")
        
        # Track statistics for this folder
        album_matches = 0
        partial_matches = 0
        unmatched_count = 0
        track_uris_for_playlist = []
        seen_albums = set()  # De-duplication within folder
        
        # Process each release
        for idx, release in enumerate(releases, 1):
            release_id = release["release_id"]
            album_title = release["album_title"]
            artist_name = release["artist_name"]
            year = release.get("year")
            discogs_url = release["discogs_url"]
            
            print(f"\n[{idx}/{len(releases)}] {artist_name} - {album_title}")
            
            # Skip if we've already processed this album (de-duplication)
            album_key = (album_title.lower(), artist_name.lower())
            if album_key in seen_albums:
                print(f"  Skipping duplicate album")
                continue
            seen_albums.add(album_key)
            
            # Try album-level match
            album_id, album_data = spotify_search_album(album_title, artist_name, year, sp=sp)
            
            if album_id:
                # Album matched - get all tracks
                print(f"  ✓ Album matched on Spotify")
                album_tracks = spotify_get_album_tracks(album_id, sp=sp)
                
                if album_tracks:
                    # Filter out tracks already added (de-duplication)
                    new_tracks = [uri for uri in album_tracks if uri not in all_track_uris]
                    track_uris_for_playlist.extend(new_tracks)
                    all_track_uris.update(new_tracks)
                    album_matches += 1
                    print(f"  Added {len(new_tracks)} tracks from album")
                else:
                    print(f"  Warning: Album matched but no tracks found")
                    unmatched_count += 1
                    unmatched_albums.append({
                        "folder_name": folder_name,
                        "discogs_release_id": release_id,
                        "discogs_url": discogs_url,
                        "album_title": album_title,
                        "artist_name": artist_name,
                        "notes": "Album matched but no tracks available"
                    })
            else:
                # Album not matched - try track-level fallback
                print(f"  Album not found, trying track-level matching...")
                tracklist = discogs_get_release_tracklist(release_id)
                
                if not tracklist:
                    print(f"  No tracklist available on Discogs")
                    unmatched_count += 1
                    unmatched_albums.append({
                        "folder_name": folder_name,
                        "discogs_release_id": release_id,
                        "discogs_url": discogs_url,
                        "album_title": album_title,
                        "artist_name": artist_name,
                        "notes": "Album not found, no tracklist available"
                    })
                    continue
                
                matched_tracks = []
                for track in tracklist:
                    track_title = track.get("title", "").strip()
                    if not track_title:
                        continue
                    
                    track_uri, _ = spotify_search_track(track_title, artist_name, album_title, sp=sp)
                    if track_uri and track_uri not in all_track_uris:
                        matched_tracks.append(track_uri)
                        all_track_uris.add(track_uri)
                    else:
                        # Track not matched - add to unmatched tracks list
                        unmatched_tracks.append({
                            "folder_name": folder_name,
                            "discogs_release_id": release_id,
                            "discogs_url": discogs_url,
                            "album_title": album_title,
                            "artist_name": artist_name,
                            "track_title": track_title,
                            "track_position": track.get("position", ""),
                            "notes": "Track not found on Spotify"
                        })
                
                if matched_tracks:
                    track_uris_for_playlist.extend(matched_tracks)
                    partial_matches += 1
                    print(f"  ✓ Matched {len(matched_tracks)}/{len(tracklist)} tracks")
                else:
                    unmatched_count += 1
                    unmatched_albums.append({
                        "folder_name": folder_name,
                        "discogs_release_id": release_id,
                        "discogs_url": discogs_url,
                        "album_title": album_title,
                        "artist_name": artist_name,
                        "notes": "Album not found, no tracks matched"
                    })
                    print(f"  ✗ No tracks matched")
            
            time.sleep(0.3)  # Rate limiting between releases
        
        # Add tracks to playlist
        if track_uris_for_playlist:
            print(f"\nAdding {len(track_uris_for_playlist)} tracks to playlist...")
            added_count = spotify_add_tracks_to_playlist(playlist_id, track_uris_for_playlist, sp=sp)
            print(f"Successfully added {added_count} tracks to playlist")
        else:
            print(f"No tracks to add to playlist")
        
        # Summary for this folder
        print(f"\n{'='*80}")
        print(f"Folder '{folder_name}' Summary:")
        print(f"  Playlist: {playlist_url}")
        print(f"  Albums fully matched (album-level): {album_matches}")
        print(f"  Albums partially matched (track-level): {partial_matches}")
        print(f"  Albums unmatched: {unmatched_count}")
        print(f"  Total tracks added: {len(track_uris_for_playlist)}")
        print(f"{'='*80}\n")
    
    # Write unmatched CSVs
    if unmatched_albums:
        unmatched_df = pd.DataFrame(unmatched_albums)
        unmatched_csv = "unmatched_albums.csv"
        unmatched_df.to_csv(unmatched_csv, index=False)
        print(f"\nWrote {len(unmatched_albums)} unmatched albums to {unmatched_csv}")
        print("You can manually review and add these to Spotify later.")
    else:
        print("\nAll albums were matched successfully!")
    
    if unmatched_tracks:
        unmatched_tracks_df = pd.DataFrame(unmatched_tracks)
        unmatched_tracks_csv = "unmatched_tracks.csv"
        unmatched_tracks_df.to_csv(unmatched_tracks_csv, index=False)
        print(f"\nWrote {len(unmatched_tracks)} unmatched tracks to {unmatched_tracks_csv}")
        print("You can manually review and add these to Spotify later.")
    else:
        print("\nAll tracks were matched successfully!")


# --------------- Vision (SYNC) ---------------

def chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def run_vision_sync(vision_client, requests_list, uris):
    """
    Call batch_annotate_images in chunks and return response dicts
    with context.uri injected so downstream code can read it uniformly.
    """
    all_responses = []
    idx = 0
    total_batches = (len(requests_list) + VISION_SYNC_CHUNK - 1) // VISION_SYNC_CHUNK
    batch_num = 0
    
    for chunk in chunked(requests_list, VISION_SYNC_CHUNK):
        batch_num += 1
        resp = vision_client.batch_annotate_images(requests=chunk)
        print(f"Processing batch {batch_num}/{total_batches}...")
        dicts = []
        for r in resp.responses:
            try:
                # Try the current approach first
                dicts.append(MessageToDict(r._pb))
            except AttributeError:
                # Fallback if _pb doesn't exist (API change)
                dicts.append(MessageToDict(r))
        if len(dicts) != len(chunk):
            print(f"WARNING: expected {len(chunk)} responses, got {len(dicts)}")
        for d in dicts:
            if "context" not in d:
                d["context"] = {}
            d["context"]["uri"] = uris[idx] if idx < len(uris) else None
            idx += 1
        all_responses.extend(dicts)
    return all_responses


# ----------------- Main -----------------

def main(update_conditions_only=False, organize_folders_only=False, test_discogs_match=False, build_spotify_playlists_only=False):
    """
    Main function to process images and update Discogs collection.
    
    Args:
        update_conditions_only: If True, skip Vision API, Discogs search, and add-to-collection,
                                and only run the condition update step.
        organize_folders_only: If True, skip Vision API and Discogs search, start from folder
                               creation and organization, then update conditions.
        test_discogs_match: If True, process first 10 images, show results, but don't write CSV
                            or proceed with collection updates.
        build_spotify_playlists_only: If True, skip all other steps and only build Spotify playlists.
    """
    if build_spotify_playlists_only:
        print("Running in build-spotify-playlists-only mode...")
        build_spotify_playlists()
        return
    
    if organize_folders_only:
        print("Running in organize-folders-only mode...")
        # Skip Vision API and Discogs search, start from folder organization
        if not DISCOGS_USER or not DISCOGS_TOKEN:
            print("Skipping folder organization: DISCOGS_USER or DISCOGS_TOKEN not set.")
            return
        
        # Load existing CSV to get owner information
        csv_file = "records.csv"
        if not os.path.exists(csv_file):
            print(f"Error: {csv_file} not found. Run the full script first to generate it.")
            return
        
        df = pd.read_csv(csv_file)
        print(f"Loaded {len(df)} records from {csv_file}")
        
        # Build mapping of release_id -> owner for matched releases
        matched_df = df[df["status"] == "matched"]
        release_to_owner = {}
        for _, row in matched_df.iterrows():
            rid = row.get("discogs_release_id")
            owner = row.get("owner", "")
            if rid and owner:
                try:
                    release_to_owner[int(rid)] = owner
                except (ValueError, TypeError):
                    continue
        
        if not release_to_owner:
            print("No matched releases found in CSV. Nothing to organize.")
            return
        
        # Create folders and organize releases
        unique_owners = set(release_to_owner.values())
        print(f"Found {len(unique_owners)} unique owners: {', '.join(sorted(unique_owners))}")
        
        # Create folders for each owner
        owner_folders = {}
        for owner in unique_owners:
            if not owner:
                continue
            folder_id = discogs_get_or_create_folder(DISCOGS_USER, owner)
            if folder_id:
                owner_folders[owner] = folder_id
                time.sleep(0.5)  # Rate limiting
        
        # Move releases to appropriate folders
        moved_count = 0
        total_to_move = len(release_to_owner)
        found_in_uncategorized = 0
        
        for move_idx, (rid, owner) in enumerate(release_to_owner.items(), 1):
            if not owner or owner not in owner_folders:
                continue
            
            try:
                # Search Uncategorized folder (where new releases are added by default)
                instance_id, current_folder_id = discogs_get_instance_for_release(DISCOGS_USER, rid, folder_id=1)
                if not instance_id:
                    continue
                
                found_in_uncategorized += 1
                target_folder_id = owner_folders[owner]
                if current_folder_id != target_folder_id:
                    success = discogs_move_instance(
                        DISCOGS_USER, rid, instance_id, 
                        current_folder_id, target_folder_id
                    )
                    if success:
                        moved_count += 1
                    if move_idx % 10 == 0 or move_idx == total_to_move:
                        print(f"Moved {move_idx}/{total_to_move} releases...")
                    time.sleep(0.8)  # Rate limiting
                else:
                    # Already in correct folder
                    moved_count += 1
            except Exception as e:
                print(f"Warning: Failed to move release {rid} to folder '{owner}': {e}")
        
        if found_in_uncategorized == 0:
            print("No records found in Uncategorized folder. Nothing to move.")
        else:
            print(f"Moved {moved_count} releases to owner folders.")
        
        # Continue to condition update
        print("\nChecking collection for items with null conditions...")
        instances = discogs_list_all_collection_instances(DISCOGS_USER)
        
        if not instances:
            print("No collection items found or field IDs not available.")
            return
        
        print(f"Found {len(instances)} items in collection. Checking for null conditions...")
        
        updated_count = 0
        for instance in instances:
            media = instance.get("media_condition")
            sleeve = instance.get("sleeve_condition")
            
            # Check if either condition is null/empty
            needs_media = not media or (isinstance(media, str) and media.strip() == "")
            needs_sleeve = not sleeve or (isinstance(sleeve, str) and sleeve.strip() == "")
            
            if needs_media or needs_sleeve:
                try:
                    instance_id = instance.get("instance_id")
                    release_id = instance.get("release_id")
                    folder_id = instance.get("folder_id")
                    
                    # Validate we have valid IDs and instance_id != release_id
                    if not instance_id or not release_id or not folder_id:
                        continue
                    if instance_id == release_id:
                        continue
                    
                    discogs_update_instance_condition(
                        DISCOGS_USER, folder_id, release_id, instance_id,
                        media_condition=DISCOGS_MEDIA_CONDITION if needs_media else None,
                        sleeve_condition=DISCOGS_SLEEVE_CONDITION if needs_sleeve else None
                    )
                    updated_count += 1
                    if updated_count % 10 == 0:
                        print(f"Updated {updated_count} items with default conditions...")
                    time.sleep(1.1)  # Rate limiting
                except Exception as e:
                    error_msg = str(e)
                    if "404" in error_msg:
                        continue
                    print(f"Failed to update instance {instance.get('instance_id')} (release {instance.get('release_id')}): {e}")
        
        if updated_count > 0:
            print(f"Updated {updated_count} collection items with default conditions.")
        else:
            print("No items found with null conditions.")
        
        # Automatically run Spotify playlist building after condition updates
        build_spotify_playlists()
        return
    
    if update_conditions_only:
        print("Running in condition-update-only mode...")
        # Skip directly to condition update section
        if not DISCOGS_USER or not DISCOGS_TOKEN:
            print("Skipping condition update: DISCOGS_USER or DISCOGS_TOKEN not set.")
            return
        
        # ---- Update collection items with null conditions ----
        print("Checking collection for items with null conditions...")
        # Get all collection instances
        instances = discogs_list_all_collection_instances(DISCOGS_USER)
        
        if not instances:
            print("No collection items found or field IDs not available.")
            return
        
        print(f"Found {len(instances)} items in collection. Checking for null conditions...")
        
        updated_count = 0
        for instance in instances:
            media = instance.get("media_condition")
            sleeve = instance.get("sleeve_condition")
            
            # Check if either condition is null/empty
            needs_media = not media or (isinstance(media, str) and media.strip() == "")
            needs_sleeve = not sleeve or (isinstance(sleeve, str) and sleeve.strip() == "")
            
            if needs_media or needs_sleeve:
                try:
                    instance_id = instance.get("instance_id")
                    release_id = instance.get("release_id")
                    folder_id = instance.get("folder_id")
                    
                    # Validate we have valid IDs and instance_id != release_id
                    if not instance_id or not release_id or not folder_id:
                        continue
                    if instance_id == release_id:
                        # This shouldn't happen if extraction is correct, but skip to avoid 404
                        continue
                    
                    discogs_update_instance_condition(
                        DISCOGS_USER, folder_id, release_id, instance_id,
                        media_condition=DISCOGS_MEDIA_CONDITION if needs_media else None,
                        sleeve_condition=DISCOGS_SLEEVE_CONDITION if needs_sleeve else None
                    )
                    updated_count += 1
                    if updated_count % 10 == 0:
                        print(f"Updated {updated_count} items with default conditions...")
                    time.sleep(1.1)  # Rate limiting
                except Exception as e:
                    error_msg = str(e)
                    # Skip 404s and continue processing
                    if "404" in error_msg:
                        # Print debug info for first few 404s
                        if updated_count < 3:
                            print(f"DEBUG 404: instance_id={instance.get('instance_id')}, release_id={instance.get('release_id')}, folder_id={instance.get('folder_id')}")
                        continue
                    print(f"Failed to update instance {instance.get('instance_id')} (release {instance.get('release_id')}): {e}")
        
        if updated_count > 0:
            print(f"Updated {updated_count} collection items with default conditions.")
        else:
            print("No items found with null conditions.")
        
        # Automatically run Spotify playlist building after condition updates
        build_spotify_playlists()
        return
    
    # ---- List images in bucket under INPUT_PREFIX ----
    try:
        gcs = storage.Client()
        bucket = gcs.bucket(GCS_BUCKET)
        imgs = [b.name for b in bucket.list_blobs(prefix=INPUT_PREFIX)
                if b.name.lower().endswith((".jpg", ".jpeg", ".png"))]
    except DefaultCredentialsError as e:
        creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "not set")
        raise SystemExit(
            f"GCS authentication failed. Check GOOGLE_APPLICATION_CREDENTIALS={creds_path}\n"
            f"Error: {e}"
        )
    except NotFound as e:
        raise SystemExit(f"GCS bucket '{GCS_BUCKET}' not found or doesn't exist: {e}")
    except Forbidden as e:
        raise SystemExit(
            f"Permission denied accessing GCS bucket '{GCS_BUCKET}'. "
            f"Check that the service account has Storage Object Viewer permission.\n"
            f"Error: {e}"
        )
    except Exception as e:
        raise SystemExit(f"Failed to access GCS bucket {GCS_BUCKET}: {type(e).__name__}: {e}")
    
    if not imgs:
        raise SystemExit(f"No images found under gs://{GCS_BUCKET}/{INPUT_PREFIX}")
    print(f"Found {len(imgs)} images under gs://{GCS_BUCKET}/{INPUT_PREFIX}")

    # ---- Vision (SYNC: Web + Text; no GCS output needed) ----
    # Load existing Vision results
    vision_cache = load_vision_cache()
    print(f"Loaded {len(vision_cache)} cached Vision results.")
    
    # In test mode, limit to first 10 images for processing
    if test_discogs_match:
        imgs = imgs[:10]
        print(f"TEST MODE: Limiting to first 10 images")
    
    # Determine which images need Vision API calls
    imgs_to_process = []
    cached_responses = []
    
    for name in imgs:
        uri = gcs_uri(name)
        cached_result = get_vision_result(vision_cache, uri)
        if cached_result:
            # Use cached result
            cached_responses.append(cached_result)
        else:
            # Need to process this image
            imgs_to_process.append((name, uri))
    
    print(f"Found {len(imgs_to_process)} new images to process with Vision API")
    print(f"Using {len(cached_responses)} cached Vision results")
    
    # In test mode, if we have enough cached results, skip Vision API calls
    if test_discogs_match and len(cached_responses) >= 10:
        print(f"TEST MODE: Using only cached results (no new Vision API calls needed)")
        imgs_to_process = []
    
    # Process new images with Vision API
    new_responses = []
    if imgs_to_process:
        vision_client = vision.ImageAnnotatorClient()
        features = [
            vision.Feature(type_=vision.Feature.Type.WEB_DETECTION,  max_results=10),
            vision.Feature(type_=vision.Feature.Type.TEXT_DETECTION, max_results=10),
        ]

        requests_list, src_uris = [], []
        for name, uri in imgs_to_process:
            try:
                content = bucket.blob(name).download_as_bytes()  # runtime SA reads; no Vision SA needed
            except Exception as e:
                print(f"WARNING: Failed to download {name}: {e}. Skipping.")
                continue
            src_uris.append(uri)
            requests_list.append(
                vision.AnnotateImageRequest(
                    image=vision.Image(content=content),
                    features=features
                )
            )

        if requests_list:
            print(f"Submitting {len(requests_list)} images to Vision API…")
            new_responses = run_vision_sync(vision_client, requests_list, src_uris)
            print(f"Got {len(new_responses)} responses from Vision.")
            
            # Save new results to cache immediately
            for resp in new_responses:
                src_uri = (resp.get("context") or {}).get("uri")
                if src_uri:
                    set_vision_result(vision_cache, src_uri, resp)
            save_vision_cache(vision_cache)
    
    # Combine cached and new responses
    resp_dicts = cached_responses + new_responses
    print(f"Total Vision responses: {len(resp_dicts)} (cached: {len(cached_responses)}, new: {len(new_responses)})")

    # In test mode, ensure we only process first 10 (should already be limited, but double-check)
    if test_discogs_match:
        resp_dicts = resp_dicts[:10]
        print(f"TEST MODE: Processing {len(resp_dicts)} images for review (using cache when available)")

    # ---- Process Vision responses ----
    rows = []
    summary = {"matched": 0, "review_needed": 0, "errors": 0}
    total_images = len(resp_dicts)
    print(f"Processing {total_images} images with Discogs API…")

    for idx, resp in enumerate(resp_dicts, 1):
        if idx % 10 == 0 or idx == total_images:
            print(f"Processing image {idx}/{total_images}...")
        src_uri = (resp.get("context") or {}).get("uri")
        image_filename = filename_from_gcs_uri(src_uri) if src_uri else ""
        owner = owner_from_gcs_uri(src_uri)

        # Per-response Vision errors
        err = resp.get("error")
        if err:
            summary["errors"] += 1
            rows.append({
                "owner": owner,
                "image_filename": image_filename,
                "image_gcs_uri": src_uri,
                "status": "review_needed",
                "confidence_level": "unknown",
                "discogs_release_id": None,
                "discogs_url": None,
                "candidate_source": "none",
                "has_discogs_candidate": False,
                "candidate_discogs_urls_top3": None,
                "candidate_other_urls_top3": None,
                "artist_hint": None,
                "album_hint": None,
                "best_guess_label": None,
                "error_message": err.get("message")
            })
            continue

        web = resp.get("webDetection", {}) or {}
        discogs_candidates, other_candidates = split_top_candidate_urls(web, limit=10)
        has_discogs = bool(discogs_candidates)

        release_id = None
        match_url = None
        match_method = None
        is_vinyl = False
        is_us = False
        match_reason = ""
        artist_hint = album_hint = None

        # A) Harvest Discogs candidates from Vision (up to 10)
        release_candidates = []
        master_candidates = []
        
        for page in web.get("pagesWithMatchingImages", [])[:10]:
            url = page.get("url") or ""
            mtype, rid = extract_release_or_master(url)
            if mtype == "release" and rid:
                release_candidates.append((rid, url))
            elif mtype == "master" and rid:
                master_candidates.append((rid, url))

        # B) Try release URLs first (validate vinyl+US)
        for rid, url in release_candidates:
            img_context = f"image {idx}/{total_images}"
            release_data = discogs_get_release(rid, context=img_context)
            if release_data:
                is_vinyl, is_us, reason = validate_release_is_vinyl_and_us(release_data)
                if is_vinyl and is_us:
                    release_id = rid
                    match_url = url
                    match_method = "release_url"
                    match_reason = reason
                    break
                elif is_vinyl:
                    # Keep as fallback if no US vinyl found
                    if not release_id:
                        release_id = rid
                        match_url = url
                        match_method = "release_url"
                        match_reason = reason

        # C) Try master URLs (resolve with vinyl+US preference)
        if not release_id or not is_us:
            for mid, url in master_candidates:
                img_context = f"image {idx}/{total_images}"
                result = discogs_release_from_master(mid, context=img_context)
                if isinstance(result, tuple) and len(result) == 4:
                    candidate_id, candidate_vinyl, candidate_us, reason = result
                    if candidate_id:
                        if candidate_vinyl and candidate_us:
                            # Perfect match - use this
                            release_id = candidate_id
                            match_url = url
                            match_method = "master_url"
                            is_vinyl = True
                            is_us = True
                            match_reason = reason
                            break
                        elif candidate_vinyl and not release_id:
                            # Fallback if we don't have a candidate yet
                            release_id = candidate_id
                            match_url = url
                            match_method = "master_url"
                            is_vinyl = True
                            is_us = False
                            match_reason = reason
                elif result:  # Backward compatibility if old format
                    candidate_id = result
                    if candidate_id:
                        release_data = discogs_get_release(candidate_id, context=img_context)
                        if release_data:
                            is_vinyl, is_us, reason = validate_release_is_vinyl_and_us(release_data)
                            if is_vinyl and is_us:
                                release_id = candidate_id
                                match_url = url
                                match_method = "master_url"
                                match_reason = reason
                                break

        # D) Fallback: OCR + Discogs search with filters
        if not release_id:
            text_ann = resp.get("textAnnotations") or []
            text = (text_ann[0].get("description", "") if text_ann else "") or ""
            parts = [p.strip() for p in text.splitlines() if p.strip()]
            if len(parts) >= 2:
                artist_hint, album_hint = parts[0], parts[1]
            bgl = (web.get("bestGuessLabels") or [{}])[0].get("label")
            if (not album_hint) and bgl and " - " in bgl:
                try:
                    artist_hint, album_hint = [s.strip() for s in bgl.split(" - ", 1)]
                except Exception:
                    pass

            img_context = f"image {idx}/{total_images}"
            search_results = cached_discogs_search(artist_hint or "", album_hint or "", context=img_context) if (artist_hint or album_hint) else []
            
            # Validate search results - prefer vinyl+US
            for hit in search_results:
                candidate_id = hit.get("id")
                if candidate_id:
                    release_data = discogs_get_release(candidate_id, context=img_context)
                    if release_data:
                        candidate_vinyl, candidate_us, reason = validate_release_is_vinyl_and_us(release_data)
                        if candidate_vinyl and candidate_us:
                            release_id = candidate_id
                            match_url = hit.get("uri")
                            match_method = "search_fallback"
                            is_vinyl = True
                            is_us = True
                            match_reason = reason
                            break
                        elif candidate_vinyl and not release_id:
                            # Fallback if no US vinyl found
                            release_id = candidate_id
                            match_url = hit.get("uri")
                            match_method = "search_fallback"
                            is_vinyl = True
                            is_us = False
                            match_reason = reason

        # Determine status and confidence
        if release_id:
            # Validate final choice if not already validated
            if not match_reason:
                release_data = discogs_get_release(release_id, context=f"image {idx}/{total_images}")
                if release_data:
                    is_vinyl, is_us, match_reason = validate_release_is_vinyl_and_us(release_data)
            
            # Mark as review_needed if not vinyl
            if not is_vinyl:
                status = "review_needed"
            else:
                status = "matched"
        else:
            status = "review_needed"
        
        confidence_level = confidence_bucket(match_method or "unknown", has_discogs, is_vinyl, is_us)
        if status == "review_needed" and (not has_discogs) and other_candidates:
            confidence_level = "very_low"

        summary[status] += 1

        rows.append({
            "owner": owner,
            "image_filename": image_filename,
            "image_gcs_uri": src_uri,
            "status": status,                               # matched | review_needed
            "confidence_level": confidence_level,           # high | medium | low | very_low | unknown
            "discogs_release_id": release_id,
            "discogs_url": match_url,
            "candidate_source": ("discogs" if has_discogs else ("other" if other_candidates else "none")),
            "has_discogs_candidate": has_discogs,
            "candidate_discogs_urls_top3": "; ".join(discogs_candidates[:3]) if discogs_candidates else None,
            "candidate_other_urls_top3": "; ".join(other_candidates) if (other_candidates and not has_discogs) else None,
            "artist_hint": artist_hint,
            "album_hint": album_hint,
            "best_guess_label": ((web.get("bestGuessLabels") or [{}])[0].get("label")),
            "error_message": None,
            "match_reason": match_reason if release_id else None
        })

    print(f"Vision summary → matched: {summary['matched']}, review_needed: {summary['review_needed']}, errors: {summary['errors']}")

    # In test mode, show results and exit
    if test_discogs_match:
        print("\n" + "="*80)
        print("TEST MODE RESULTS - First 10 images:")
        print("="*80)
        for i, row in enumerate(rows, 1):
            print(f"\n{i}. {row.get('image_filename', 'unknown')}")
            print(f"   Status: {row.get('status')}")
            print(f"   Confidence: {row.get('confidence_level')}")
            if row.get('discogs_release_id'):
                print(f"   Release ID: {row.get('discogs_release_id')}")
                print(f"   URL: {row.get('discogs_url')}")
                print(f"   Reason: {row.get('match_reason', 'N/A')}")
            else:
                print(f"   Artist hint: {row.get('artist_hint')}")
                print(f"   Album hint: {row.get('album_hint')}")
                if row.get('candidate_discogs_urls_top3'):
                    print(f"   Discogs candidates: {row.get('candidate_discogs_urls_top3')}")
        print("\n" + "="*80)
        print("Test mode complete. No CSV written, no collection updates performed.")
        return

    # ---- Write CSV (pre-dedup) ----
    df = pd.DataFrame(rows)

    # ---- De-dup: mark what's already in your collection and skip adding them ----
    if DISCOGS_USER and DISCOGS_TOKEN:
        print("Fetching existing collection to avoid duplicates…")
        existing_ids = discogs_list_all_collection_release_ids(DISCOGS_USER)
        print(f"Found {len(existing_ids)} releases already in your collection.")
        df["already_in_collection"] = df["discogs_release_id"].apply(
            lambda x: (int(x) in existing_ids) if pd.notna(x) else False
        )
    else:
        df["already_in_collection"] = False

    out_csv = "records.csv"
    df.to_csv(out_csv, index=False)
    print(f"Wrote {len(df)} rows to {out_csv}")

    # ---- Add matched releases to Discogs collection (skip duplicates) ----
    if not DISCOGS_USER or not DISCOGS_TOKEN:
        print("Skipping Discogs add: DISCOGS_USER or DISCOGS_TOKEN not set.")
        return

    add_mask = (df["status"] == "matched") & (~df["already_in_collection"])
    # Build mapping of release_id -> owner for folder organization
    release_to_owner = {}
    for _, row in df.loc[add_mask].iterrows():
        rid = row.get("discogs_release_id")
        owner = row.get("owner", "")
        if rid and owner:
            try:
                release_to_owner[int(rid)] = owner
            except (ValueError, TypeError):
                continue
    
    to_add = list(release_to_owner.keys())
    skipped_dupes = int((df["status"] == "matched").sum() - len(to_add))

    print(f"Adding {len(to_add)} releases (skipped {skipped_dupes} already in your collection)…")
    added = 0
    total_to_add = len(to_add)
    
    for add_idx, rid in enumerate(to_add, 1):
        try:
            # Check if already in collection (search Uncategorized folder where new releases are added)
            instance_id, actual_folder_id = discogs_get_instance_for_release(DISCOGS_USER, rid, folder_id=1)
            
            if not instance_id:
                # Add to collection (conditions will be set at the end)
                discogs_add_to_collection(DISCOGS_USER, rid, DISCOGS_FOLDER_ID)
                time.sleep(0.6)  # Rate limiting
                # Get instance_id after adding (it will be in Uncategorized folder)
                instance_id, actual_folder_id = discogs_get_instance_for_release(DISCOGS_USER, rid, folder_id=1)
            
            added += 1
            if add_idx % 5 == 0 or add_idx == total_to_add:
                print(f"Added {add_idx}/{total_to_add} releases...")
            time.sleep(1.1)  # ~60/min pacing
        except Exception as e:
            error_msg = str(e)
            # If it's a 409 (already exists), that's fine
            if "409" in error_msg or "already" in error_msg.lower():
                added += 1  # Count as success
                continue
            print(f"Add failed for release {rid} ({add_idx}/{total_to_add}): {e}")
    print(f"Added {added} releases.")
    
    # ---- Create folders and organize releases by owner ----
    print("Organizing releases into owner folders...")
    if DISCOGS_USER and DISCOGS_TOKEN:
        # Get unique owners from releases we just added
        unique_owners = set(release_to_owner.values())
        
        # Create folders for each owner
        owner_folders = {}
        for owner in unique_owners:
            if not owner:
                continue
            folder_id = discogs_get_or_create_folder(DISCOGS_USER, owner)
            if folder_id:
                owner_folders[owner] = folder_id
                time.sleep(0.5)  # Rate limiting
        
        # Move releases to appropriate folders
        moved_count = 0
        found_in_uncategorized = 0
        
        for rid, owner in release_to_owner.items():
            if not owner or owner not in owner_folders:
                continue
            
            try:
                # Search Uncategorized folder (where new releases are added by default)
                instance_id, current_folder_id = discogs_get_instance_for_release(DISCOGS_USER, rid, folder_id=1)
                if not instance_id:
                    continue
                
                found_in_uncategorized += 1
                target_folder_id = owner_folders[owner]
                if current_folder_id != target_folder_id:
                    success = discogs_move_instance(
                        DISCOGS_USER, rid, instance_id, 
                        current_folder_id, target_folder_id
                    )
                    if success:
                        moved_count += 1
                    time.sleep(0.8)  # Rate limiting
            except Exception as e:
                print(f"Warning: Failed to move release {rid} to folder '{owner}': {e}")
        
        if found_in_uncategorized == 0:
            print("No records found in Uncategorized folder. Nothing to move.")
        else:
            print(f"Moved {moved_count} releases to owner folders.")
    
    # ---- Update collection items with null conditions ----
    print("Checking collection for items with null conditions...")
    if DISCOGS_USER and DISCOGS_TOKEN:
        # Get all collection instances (using folder 0 = All)
        instances = discogs_list_all_collection_instances(DISCOGS_USER)
        
        if not instances:
            print("No collection items found or field IDs not available.")
            return
        
        print(f"Found {len(instances)} items in collection. Checking for null conditions...")
        
        updated_count = 0
        for instance in instances:
            media = instance.get("media_condition")
            sleeve = instance.get("sleeve_condition")
            
            # Check if either condition is null/empty
            needs_media = not media or (isinstance(media, str) and media.strip() == "")
            needs_sleeve = not sleeve or (isinstance(sleeve, str) and sleeve.strip() == "")
            
            if needs_media or needs_sleeve:
                try:
                    instance_id = instance.get("instance_id")
                    release_id = instance.get("release_id")
                    folder_id = instance.get("folder_id")
                    
                    # Validate we have valid IDs and instance_id != release_id
                    if not instance_id or not release_id or not folder_id:
                        continue
                    if instance_id == release_id:
                        # This shouldn't happen if extraction is correct, but skip to avoid 404
                        continue
                    
                    discogs_update_instance_condition(
                        DISCOGS_USER, folder_id, release_id, instance_id,
                        media_condition=DISCOGS_MEDIA_CONDITION if needs_media else None,
                        sleeve_condition=DISCOGS_SLEEVE_CONDITION if needs_sleeve else None
                    )
                    updated_count += 1
                    if updated_count % 10 == 0:
                        print(f"Updated {updated_count} items with default conditions...")
                    time.sleep(1.1)  # Rate limiting
                except Exception as e:
                    error_msg = str(e)
                    # Skip 404s and continue processing
                    if "404" in error_msg:
                        # Print debug info for first few 404s
                        if updated_count < 3:
                            print(f"DEBUG 404: instance_id={instance.get('instance_id')}, release_id={instance.get('release_id')}, folder_id={instance.get('folder_id')}")
                        continue
                    print(f"Failed to update instance {instance.get('instance_id')} (release {instance.get('release_id')}): {e}")
        
        if updated_count > 0:
            print(f"Updated {updated_count} collection items with default conditions.")
        else:
            print("No items found with null conditions.")
        
        # Automatically run Spotify playlist building after condition updates
        build_spotify_playlists()

if __name__ == "__main__":
    # Basic sanity checks
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path:
        raise SystemExit("GOOGLE_APPLICATION_CREDENTIALS not set. Export the path to your service-account JSON.")
    # Expand environment variables in the path (e.g., $HOME, ~)
    creds_path = os.path.expanduser(os.path.expandvars(creds_path))
    
    # If the specified path doesn't exist, try looking in the script's directory
    if not os.path.exists(creds_path):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        local_creds = os.path.join(script_dir, "gcp-service-key.json")
        if os.path.exists(local_creds):
            print(f"Note: Using credentials file found in script directory: {local_creds}")
            creds_path = local_creds
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
        else:
            raise SystemExit(
                f"Credentials file not found at: {creds_path}\n"
                f"Also checked: {local_creds}\n"
                f"Please set GOOGLE_APPLICATION_CREDENTIALS to the correct path."
            )
    
    if not os.access(creds_path, os.R_OK):
        raise SystemExit(f"Credentials file is not readable: {creds_path}\nCheck file permissions.")
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process vinyl images and update Discogs collection')
    parser.add_argument('--update-conditions-only', action='store_true',
                        help='Skip Vision API, Discogs search, and add-to-collection. Only update null conditions.')
    parser.add_argument('--organize-folders-only', action='store_true',
                        help='Skip Vision API and Discogs search. Start from folder creation/organization, then update conditions.')
    parser.add_argument('--test-discogs-match', action='store_true',
                        help='Process first 10 images, show Discogs match results, but do not write CSV or update collection.')
    parser.add_argument('--build-spotify-playlists', action='store_true',
                        help='Skip all other steps and only build Spotify playlists from Discogs collection folders.')
    parser.add_argument('--input-prefix', type=str, default=None,
                        help='GCS prefix/path to process images from (e.g., "covers/Owner/" or "covers/2024/January/"). Overrides VINYL_INPUT_PREFIX env var.')
    args = parser.parse_args()
    
    # Validate flag combinations
    flags_set = sum([args.update_conditions_only, args.organize_folders_only, args.test_discogs_match, args.build_spotify_playlists])
    if flags_set > 1:
        raise SystemExit("Cannot use multiple mode flags together. Choose one: --update-conditions-only, --organize-folders-only, --test-discogs-match, or --build-spotify-playlists")
    
    if not args.update_conditions_only and not args.organize_folders_only and not args.test_discogs_match and not args.build_spotify_playlists and not GCS_BUCKET:
        raise SystemExit("GCS_BUCKET is empty; set it at the top of the script.")
    
    # Override INPUT_PREFIX if --input-prefix argument is provided
    if args.input_prefix:
        input_prefix = args.input_prefix.strip()
        # Skip override if input becomes empty after stripping (e.g., whitespace-only input)
        if input_prefix:
            # Ensure it ends with / if not empty
            if not input_prefix.endswith('/'):
                input_prefix = input_prefix + '/'
            # Update the module-level INPUT_PREFIX for this run
            globals()['INPUT_PREFIX'] = input_prefix
            print(f"Using input prefix from command line: {input_prefix}")
    
    main(update_conditions_only=args.update_conditions_only, 
         organize_folders_only=args.organize_folders_only,
         test_discogs_match=args.test_discogs_match,
         build_spotify_playlists_only=args.build_spotify_playlists)
