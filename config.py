"""
Configuration module for vinyl_bulk.
Handles environment variable loading and configuration setup.
"""

import os

# --- Auto-load .env early (never overwrite real env) ---
try:
    from dotenv import load_dotenv, find_dotenv
except ModuleNotFoundError:
    raise SystemExit(
        "Missing dependency: python-dotenv. Install it with:\n"
        "  pip install python-dotenv"
    )

# Load order: .env.local (highest), .env (fallback). Do NOT override existing env.
# This lets prod/CI env vars win, and local shells still "just work".
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

