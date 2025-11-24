# vinyl-bulk

Bulk-match record cover images against Discogs using Google Cloud Vision (Web + OCR), export results to CSV, and optionally add matched releases to your Discogs collection. Runs **synchronously** and reads images from GCS using your runtime service account (no Vision service agent or GCS output prefix required).

## Features

- Batch Web Detection + OCR from local bytes (no gs:// read by Vision)
- Discogs matching: direct release/master URL → master→release resolver → OCR fallback search
- Vinyl + US bias: prefers vinyl releases and US pressings when matching
- Confidence buckets; candidate URLs captured for manual review
- De-dup: fetches your collection and skips already-owned releases
- Retries & backoff for Discogs (429/5xx), per-image error capture to CSV
- Spotify playlist builder: automatically creates playlists from Discogs collection folders

## Prerequisites

- Python 3.9+ (tested on 3.10+)
- A GCP project with **Vision API** enabled
- A **service account key JSON** with read access to your bucket objects (Storage Object Viewer is enough)
- Discogs API token (get one at [discogs.com/settings/developers](https://www.discogs.com/settings/developers))
- Spotify API credentials (optional, for playlist building)

## Installation

```bash
python -m venv .venv && source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## Configuration

Copy `.env.example` to `.env` and fill in your values, or export environment variables directly.

### Required Environment Variables

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/abs/path/to/service-account.json
export DISCOGS_USER=your_discogs_username
export DISCOGS_TOKEN=your_discogs_token
export VINYL_GCS_BUCKET=your-bucket
```

### Optional: Discogs User-Agent

These help Discogs trust your client:

```bash
export DISCOGS_APP_NAME="vinyl-bulk"
export DISCOGS_APP_VERSION="1.1"
# export DISCOGS_CONTACT="Your Name <you@example.com>"  # optional
# export DISCOGS_APP_URL="https://github.com/yourrepo"  # optional
```

### Optional: Storage & Runtime Settings

```bash
export VINYL_INPUT_PREFIX=covers/
export DISCOGS_FOLDER_ID=1
export VISION_SYNC_CHUNK=8
```

**Note:** 
- `.HEIC` is not supported by Vision. Convert to JPG/PNG first.
- `VINYL_INPUT_PREFIX` can be overridden at runtime using the `--input-prefix` command-line argument (see [Specifying a Subfolder](#specifying-a-subfolder) section).

### Optional: Collection Condition Defaults

Set default media and sleeve condition ratings when adding releases to your collection:

```bash
export DISCOGS_MEDIA_CONDITION="Very Good (VG)"
export DISCOGS_SLEEVE_CONDITION="Good Plus (G+)"
```

If not set, defaults to "Very Good (VG)" for media and "Good Plus (G+)" for sleeve.

**Accepted grade strings (use exact spellings):**
- Mint (M)
- Near Mint (NM or M-)
- Very Good Plus (VG+)
- Very Good (VG)
- Good Plus (G+)
- Good (G)
- Fair (F)
- Poor (P)

**Note:** Sleeve condition sometimes supports "No Cover" as a value, but this is optional and only used if explicitly set.

**Behavior:**
- When adding new releases: Default conditions are set automatically.
- When adding releases that already exist: Only sets conditions if they are currently null/empty (preserves existing values).
- When updating existing collection: Only updates items with null/empty conditions to defaults (never overwrites existing values).

### Optional: Matching Preferences

Control the Discogs matching logic to prefer vinyl releases and US pressings:

```bash
export FORMAT_FILTER=Vinyl
export COUNTRY_PREF=US
export SEARCH_PAGE_SIZE=10
```

- `FORMAT_FILTER`: Format to prefer when matching (default: "Vinyl")
- `COUNTRY_PREF`: Country to prefer when matching (default: "US")
- `SEARCH_PAGE_SIZE`: Number of search results to consider per image (default: 10)

### Optional: Spotify Playlist Builder

Enable building Spotify playlists from your Discogs collection folders:

```bash
export SPOTIPY_CLIENT_ID=your_spotify_client_id
export SPOTIPY_CLIENT_SECRET=your_spotify_client_secret
export SPOTIPY_REDIRECT_URI=http://127.0.0.1:8888/callback
export DISCOGS_PLAYLIST_SOURCE_FOLDER=FolderName  # optional: build playlist from single folder
export SPOTIFY_PLAYLIST_URL=https://open.spotify.com/playlist/your_playlist_id  # optional: add to existing playlist
```

**Playlist Creation Modes:**

- **If `SPOTIFY_PLAYLIST_URL` is set:** Adds tracks to the specified existing playlist instead of creating new ones. The script will:
  - Fetch existing tracks from the playlist
  - Process folders and collect tracks (respects `DISCOGS_PLAYLIST_SOURCE_FOLDER` and `--input-prefix` settings)
  - **When `DISCOGS_PLAYLIST_SOURCE_FOLDER` is also set:** Only processes that specific folder and adds tracks from it
  - Only add tracks that don't already exist in the playlist (de-duplication)
  - **Never deletes tracks** from the playlist - only adds new ones
  - Skip folder-based playlist creation entirely

- **If `SPOTIFY_PLAYLIST_URL` is not set:** Creates new playlists per folder:
  - If `DISCOGS_PLAYLIST_SOURCE_FOLDER` is set, builds one playlist from that folder
  - If not set, builds one playlist per custom folder (skips system folders)

**Folder Filtering:** When using `--input-prefix` to specify a GCS subfolder, the script will automatically filter Discogs folders to only include those that match folders found in the GCS prefix. For example, if you use `--input-prefix "covers/Dad/"`, only Discogs folders that correspond to folders under that GCS path (like "Dad", "Dad_Shed", etc.) will be processed for Spotify playlists.

**Precedence Order:** When multiple folder selection options are used:
1. **`--input-prefix`** (command-line) - **Highest priority** - Takes precedence over all other settings
2. **`DISCOGS_PLAYLIST_SOURCE_FOLDER`** (environment variable) - Used only if `--input-prefix` is not set
3. **Default behavior** - Processes all custom folders if neither is set

**Combining Settings:** When using `SPOTIFY_PLAYLIST_URL` with `--input-prefix`:
- `--input-prefix` takes precedence over `DISCOGS_PLAYLIST_SOURCE_FOLDER`
- Only folders matching the GCS prefix are processed (ignores `DISCOGS_PLAYLIST_SOURCE_FOLDER` if both are set)
- Only tracks from those folders that don't already exist in the playlist are added
- Tracks are never deleted from the playlist

**Playlist URL Formats:**
- `https://open.spotify.com/playlist/{playlist_id}`
- `spotify:playlist:{playlist_id}`
- Direct playlist ID (22-character alphanumeric string)

## Usage

### Image Storage

Images should live under:
```
gs://<bucket>/<INPUT_PREFIX>/<Owner>/[<Subfolder>/...]/*.jpg
# e.g., gs://your-bucket/covers/Name1/abc.jpg, covers/Name2/xyz.jpg
# e.g., gs://your-bucket/covers/Dad/Shed/image.jpg (creates folder "Dad_Shed")
# e.g., gs://your-bucket/covers/Dad/Shed/Collection/image.jpg (creates folder "Dad_Shed_Collection")
```

**Folder Naming:** The script creates Discogs collection folders based on the directory structure after `INPUT_PREFIX`. Subfolders are joined with underscores:
- `covers/Dad/image.jpg` → folder: **"Dad"**
- `covers/Dad/Shed/image.jpg` → folder: **"Dad_Shed"**
- `covers/Dad/Shed/Collection/image.jpg` → folder: **"Dad_Shed_Collection"**

### Specifying a Subfolder

You can specify a subfolder within your GCS bucket using the `--input-prefix` argument. This is useful when you want to process images from a specific directory structure:

```bash
# Process images from a single owner folder
python vinyl_bulk.py --input-prefix "covers/Dad/"

# The prefix will automatically have a trailing slash added if not provided
python vinyl_bulk.py --input-prefix "covers/2024/January"
```

**Note:** The `--input-prefix` argument overrides the `VINYL_INPUT_PREFIX` environment variable. If not specified, the script uses the value from `VINYL_INPUT_PREFIX` (default: `"covers/"`).

### Full Run

Process all images, match with Discogs, and add to collection:

```bash
python vinyl_bulk.py
```

Or process images from a specific subfolder:

```bash
python vinyl_bulk.py --input-prefix "covers/2024/January/"
```

### Test Discogs Matching

Test the matching logic on the first 10 images without writing to CSV or updating your collection:

```bash
python vinyl_bulk.py --test-discogs-match
```

**What it shows:**
- Image filename
- Match status (matched or review_needed)
- Confidence level (high, medium, low, very_low, unknown)
- Discogs release ID and URL (if matched)
- Match reason (e.g., "Vinyl, US", "Vinyl, UK (not US)", etc.)
- Artist/album hints and candidate URLs (if no match found)

**Use cases:**
- Reviewing how the vinyl+US matching logic performs on your images
- Verifying that the matching preferences are working as expected
- Testing before running the full pipeline
- Debugging matching issues

**Requirements:** GCS credentials and bucket access. `DISCOGS_USER` and `DISCOGS_TOKEN` are optional but enable better matching via collection de-duplication.

### Condition Update Only

Update null/empty conditions on existing collection items:

```bash
python vinyl_bulk.py --update-conditions-only
```

**What it does:**
- Skips Vision API processing, Discogs search, and adding releases to collection
- Checks your existing collection and updates any items with null/empty media or sleeve conditions to the default values

**Use cases:**
- Testing condition updates without running the full pipeline
- Updating conditions on existing collection items
- Faster iteration when debugging condition update logic

**Requirements:** `DISCOGS_USER` and `DISCOGS_TOKEN`. Does not require GCS credentials or bucket access.

### Organize Folders Only

Organize existing collection items into owner-based folders:

```bash
python vinyl_bulk.py --organize-folders-only
```

**What it does:**
- Skips Vision API and Discogs search
- Reads from the existing `records.csv` file
- Creates Discogs collection folders based on the "Owner" field from the GCS bucket paths
- Moves matched releases to the appropriate owner folders
- Updates null conditions

**Use cases:**
- Organizing existing collection items into owner-based folders
- Re-running folder organization without re-processing images
- Testing folder creation and organization logic

**Requirements:** `records.csv` must exist (from a previous full run), and `DISCOGS_USER` and `DISCOGS_TOKEN` must be set. Does not require GCS credentials or bucket access.

### Build Spotify Playlists

Build Spotify playlists from your Discogs collection folders:

```bash
python vinyl_bulk.py --build-spotify-playlists
```

**What it does:**
- Prompts for confirmation before proceeding (review gate)
- Fetches releases from selected Discogs folders
- Matches albums on Spotify (with album-level matching heuristics)
- Falls back to track-level matching if album not found
- Creates playlists with format: `"<FolderName> — Discogs albums (YYYY-MM-DD)"`
- Outputs `unmatched_albums.csv` and `unmatched_tracks.csv` for items that couldn't be matched

**Use cases:**
- Building playlists from your existing Discogs collection
- Re-building playlists after adding new albums
- Testing playlist building without running the full pipeline

**Requirements:** `DISCOGS_USER`, `DISCOGS_TOKEN`, and Spotify credentials (`SPOTIPY_CLIENT_ID`, `SPOTIPY_CLIENT_SECRET`, `SPOTIPY_REDIRECT_URI`). Does not require GCS credentials or bucket access.

**Automatic execution:** The playlist builder also runs automatically at the end of the normal workflow (after condition updates), with a review gate prompt before proceeding.

## Output Files

### records.csv

One row per image with the following key columns:

- `status` - matched | review_needed
- `confidence_level` - high | medium | low | very_low | unknown
- `discogs_release_id` - Discogs release ID if matched
- `discogs_url` - URL to the matched release
- `candidate_discogs_urls_top3` - Top 3 Discogs candidate URLs found
- `candidate_other_urls_top3` - Top 3 non-Discogs candidate URLs found
- `artist_hint` - Artist name extracted from OCR
- `album_hint` - Album title extracted from OCR
- `best_guess_label` - Vision API best guess label
- `already_in_collection` - true/false
- `error_message` - Error message if Vision returned a per-image error

The script prints a summary (matched / review_needed / errors) and, if credentials are set, adds only non-duplicate matched releases to your Discogs collection with default condition ratings. After adding new releases, the script also checks your entire collection and updates any items with null/empty media or sleeve conditions to the default values.

### unmatched_albums.csv

Created when building Spotify playlists. One row per album that couldn't be matched on Spotify (no album match AND zero track matches).

**Columns:**
- `folder_name` - Discogs folder name
- `discogs_release_id` - Discogs release ID
- `discogs_url` - URL to the Discogs release
- `album_title` - Album title
- `artist_name` - Artist name
- `notes` - Reason why it wasn't matched

Use this to manually review and add albums to Spotify later.

### unmatched_tracks.csv

Created when building Spotify playlists. One row per track that couldn't be matched during track-level fallback.

**Columns:**
- `folder_name` - Discogs folder name
- `discogs_release_id` - Discogs release ID
- `discogs_url` - URL to the Discogs release
- `album_title` - Album title
- `artist_name` - Artist name
- `track_title` - Track title
- `track_position` - Track position on the release
- `notes` - Reason why it wasn't matched

Use this to manually review and add individual tracks to Spotify later.

## Architecture

### Codebase Structure

The codebase has been refactored into modular components for better maintainability:

- **`vinyl_bulk.py`** - Main entry point with CLI argument parsing and orchestration
- **`config.py`** - Environment variable loading and configuration management
- **`helpers.py`** - Utility functions (GCS URI parsing, URL extraction, confidence scoring)
- **`vision_cache.py`** - Vision API result caching to avoid redundant API calls
- **`http_client.py`** - HTTP retry logic with exponential backoff for API calls
- **`discogs_api.py`** - All Discogs API interactions (releases, collections, folders, conditions)
- **`spotify_api.py`** - Spotify API client functions (authentication, search, playlists)
- **`vision_api.py`** - Google Cloud Vision API batch processing
- **`workflows.py`** - Main processing workflows (image processing, collection updates, folder organization)
- **`spotify_playlists.py`** - Spotify playlist building workflow and orchestration

This modular structure makes the codebase:
- **Easier to maintain** - Each module has a single, clear responsibility
- **More testable** - Modules can be tested independently
- **More readable** - Smaller files are easier to understand and navigate
- **More reusable** - Modules can be imported by other scripts

### Why Sync Vision (bytes) Instead of Async (gs://)?

- No need to grant the Google-managed Vision service agent IAM on your bucket
- Fewer moving parts; everything stays inside your process
- Simpler error handling and retry logic

## Troubleshooting

### All Rows Marked as review_needed

- Confirm your images are JPEG/PNG and readable
- Start with a small batch to test
- Lower `VISION_SYNC_CHUNK` to 4 if images are large

### Many 429 Errors from Discogs

- Retries/backoff are built-in
- You can lower concurrency by running fewer images
- The script automatically respects `Retry-After` headers

### GCS Permission Errors

- Ensure the runtime service account (from `GOOGLE_APPLICATION_CREDENTIALS`) has Storage Object Viewer permission on the bucket
- Verify the credentials file path is correct and accessible

### Spotify Authentication Issues

- Ensure `SPOTIPY_CLIENT_ID`, `SPOTIPY_CLIENT_SECRET`, and `SPOTIPY_REDIRECT_URI` are set
- Verify the redirect URI in the Spotify dashboard matches exactly (use `http://127.0.0.1:8888/callback` or another allowed loopback IP)
- Check that your Spotify app has the required scopes: `playlist-modify-private` and `playlist-modify-public`

## License

MIT License

Copyright (c) 2025 Seraph Ventures LLC

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
