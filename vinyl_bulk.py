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

import os
import argparse

# Import configuration (loads .env and sets up environment)
import config

# Import workflows
from workflows import (
    main_workflow,
    update_conditions_workflow,
    organize_folders_workflow
)
from spotify_playlists import build_spotify_playlists


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
        organize_folders_workflow()
        # Continue to condition update
        update_conditions_workflow()
        # Automatically run Spotify playlist building after condition updates
        build_spotify_playlists()
        return
    
    if update_conditions_only:
        print("Running in condition-update-only mode...")
        update_conditions_workflow()
        # Automatically run Spotify playlist building after condition updates
        build_spotify_playlists()
        return
    
    # Main workflow: process images, match with Discogs, update collection
    main_workflow(test_discogs_match=test_discogs_match)


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
    
    if not args.update_conditions_only and not args.organize_folders_only and not args.test_discogs_match and not args.build_spotify_playlists and not config.GCS_BUCKET:
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
            config.INPUT_PREFIX = input_prefix
            print(f"Using input prefix from command line: {input_prefix}")
    
    main(update_conditions_only=args.update_conditions_only, 
         organize_folders_only=args.organize_folders_only,
         test_discogs_match=args.test_discogs_match,
         build_spotify_playlists_only=args.build_spotify_playlists)
