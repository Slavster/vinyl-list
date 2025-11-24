"""
Spotify playlist building workflow.
Handles building playlists from Discogs collection folders, either creating new playlists
or adding tracks to an existing playlist.
"""

import os
import time
import pandas as pd
from datetime import datetime

import config
from config import (
    DISCOGS_USER, DISCOGS_TOKEN,
    DISCOGS_PLAYLIST_SOURCE_FOLDER, SPOTIFY_PLAYLIST_URL
)
from discogs_api import (
    discogs_get_collection_folders_with_names,
    discogs_list_folder_releases,
    discogs_get_release_tracklist
)
from spotify_api import (
    spotify_authenticate,
    spotify_extract_playlist_id,
    spotify_get_playlist_tracks,
    spotify_create_playlist,
    spotify_add_tracks_to_playlist,
    spotify_search_album,
    spotify_get_album_tracks,
    spotify_search_track
)
from helpers import get_folders_from_gcs_prefix


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
        prefix_was_customized = config.INPUT_PREFIX != default_prefix
        
        # If INPUT_PREFIX was customized, filter folders based on GCS structure
        gcs_folder_names = set()
        if prefix_was_customized:
            print(f"\nINPUT_PREFIX was customized to: {config.INPUT_PREFIX}")
            print("Extracting folder names from GCS paths...")
            gcs_folder_names = get_folders_from_gcs_prefix(config.INPUT_PREFIX)
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
                print(f"Warning: No folders found in GCS under prefix '{config.INPUT_PREFIX}'. Nothing to process.")
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
            print(f"\nProcessing {len(folders_to_process)} folder(s) from GCS prefix '{config.INPUT_PREFIX}' and adding tracks to existing playlist...")
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
    prefix_was_customized = config.INPUT_PREFIX != default_prefix
    
    # If INPUT_PREFIX was customized, filter folders based on GCS structure
    gcs_folder_names = set()
    if prefix_was_customized:
        print(f"\nINPUT_PREFIX was customized to: {config.INPUT_PREFIX}")
        print("Extracting folder names from GCS paths...")
        gcs_folder_names = get_folders_from_gcs_prefix(config.INPUT_PREFIX)
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
            print(f"Warning: No folders found in GCS under prefix '{config.INPUT_PREFIX}'. Nothing to process.")
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

