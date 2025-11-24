"""
Discogs API client module.
Handles all interactions with the Discogs API including releases, collections, folders, and conditions.
"""

import time
from functools import lru_cache
from http_client import http_get_with_retry, http_post_with_retry, discogs_headers
from config import (
    DISCOGS_USER, DISCOGS_TOKEN, FORMAT_FILTER, COUNTRY_PREF, SEARCH_PAGE_SIZE,
    DISCOGS_MEDIA_CONDITION, DISCOGS_SLEEVE_CONDITION
)


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

