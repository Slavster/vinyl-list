"""
Spotify API client module.
Handles Spotify authentication, search, and playlist operations.
"""

import os
import re
import time

try:
    import spotipy
    from spotipy.oauth2 import SpotifyOAuth
    SPOTIPY_AVAILABLE = True
except ImportError:
    SPOTIPY_AVAILABLE = False


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

