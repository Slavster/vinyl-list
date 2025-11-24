"""
Main processing workflows for vinyl_bulk.
Handles image processing, Discogs matching, collection updates, and folder organization.
"""

import os
import time
import pandas as pd
from google.cloud import vision, storage
from google.cloud.exceptions import NotFound, Forbidden
from google.auth.exceptions import DefaultCredentialsError

import config
from config import (
    GCS_BUCKET, DISCOGS_USER, DISCOGS_TOKEN, DISCOGS_FOLDER_ID,
    DISCOGS_MEDIA_CONDITION, DISCOGS_SLEEVE_CONDITION
)
from helpers import gcs_uri, filename_from_gcs_uri, owner_from_gcs_uri, split_top_candidate_urls, extract_release_or_master, confidence_bucket
from vision_cache import load_vision_cache, get_vision_result, set_vision_result, save_vision_cache
from vision_api import run_vision_sync
from discogs_api import (
    discogs_get_release, validate_release_is_vinyl_and_us, discogs_release_from_master,
    cached_discogs_search, discogs_list_all_collection_release_ids,
    discogs_add_to_collection, discogs_get_instance_for_release,
    discogs_get_or_create_folder, discogs_move_instance,
    discogs_list_all_collection_instances, discogs_update_instance_condition
)
from spotify_playlists import build_spotify_playlists


def update_conditions_workflow():
    """Update collection items with null conditions."""
    if not DISCOGS_USER or not DISCOGS_TOKEN:
        print("Skipping condition update: DISCOGS_USER or DISCOGS_TOKEN not set.")
        return
    
    print("Checking collection for items with null conditions...")
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


def organize_folders_workflow():
    """Organize existing collection items into owner-based folders."""
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


def process_vision_responses(resp_dicts, test_mode=False):
    """Process Vision API responses and match with Discogs."""
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
            "status": status,
            "confidence_level": confidence_level,
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
    return rows


def add_to_collection_and_organize(release_to_owner):
    """Add releases to collection and organize into folders."""
    if not DISCOGS_USER or not DISCOGS_TOKEN:
        print("Skipping Discogs add: DISCOGS_USER or DISCOGS_TOKEN not set.")
        return
    
    to_add = list(release_to_owner.keys())
    print(f"Adding {len(to_add)} releases…")
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
    
    # Create folders and organize releases by owner
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


def main_workflow(test_discogs_match=False):
    """Main workflow: process images, match with Discogs, and update collection."""
    # List images in bucket under INPUT_PREFIX
    try:
        gcs = storage.Client()
        bucket = gcs.bucket(GCS_BUCKET)
        imgs = [b.name for b in bucket.list_blobs(prefix=config.INPUT_PREFIX)
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
        raise SystemExit(f"No images found under gs://{GCS_BUCKET}/{config.INPUT_PREFIX}")
    print(f"Found {len(imgs)} images under gs://{GCS_BUCKET}/{config.INPUT_PREFIX}")
    
    # Vision (SYNC: Web + Text; no GCS output needed)
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
    
    # Process Vision responses
    rows = process_vision_responses(resp_dicts, test_mode=test_discogs_match)
    
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
    
    # Write CSV (pre-dedup)
    df = pd.DataFrame(rows)
    
    # De-dup: mark what's already in your collection and skip adding them
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
    
    # Add matched releases to Discogs collection (skip duplicates)
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
    
    skipped_dupes = int((df["status"] == "matched").sum() - len(release_to_owner))
    print(f"Adding {len(release_to_owner)} releases (skipped {skipped_dupes} already in your collection)…")
    
    add_to_collection_and_organize(release_to_owner)
    
    # Update collection items with null conditions
    update_conditions_workflow()
    
    # Automatically run Spotify playlist building after condition updates
    build_spotify_playlists()

