"""
Helper utilities for GCS URI parsing, URL extraction, confidence scoring, and GCS operations.
"""

import re
import posixpath
from urllib.parse import urlparse
import config
from config import GCS_BUCKET
from google.cloud import storage


def gcs_uri(obj: str) -> str:
    return f"gs://{GCS_BUCKET}/{obj}"

def filename_from_gcs_uri(uri: str) -> str:
    if not uri:
        return ""
    p = urlparse(uri)
    return posixpath.basename(p.path)

def extract_owner_from_uri(uri: str) -> str:
    """Extract the owner (first folder after 'covers/') from GCS URI.
    Returns just the owner name, e.g., 'Dad' from 'covers/Dad/Shed/image.jpg'
    """
    if not uri:
        return ""
    p = urlparse(uri)
    path = p.path.lstrip("/")
    if path.startswith(GCS_BUCKET + "/"):
        path = path[len(GCS_BUCKET) + 1:]
    
    # Remove the base prefix (typically "covers/")
    base_prefix = "covers/"
    if path.startswith(base_prefix):
        rel_path = path[len(base_prefix):]
    else:
        # If path doesn't start with "covers/", try to find where folders start
        # by removing INPUT_PREFIX if it's more specific
        if path.startswith(config.INPUT_PREFIX):
            # Extract folders from INPUT_PREFIX itself
            prefix_without_base = config.INPUT_PREFIX
            if prefix_without_base.startswith(base_prefix):
                prefix_without_base = prefix_without_base[len(base_prefix):]
            prefix_parts = [p for p in prefix_without_base.rstrip("/").split("/") if p]
            if prefix_parts:
                return prefix_parts[0]  # Return first folder after "covers"
        rel_path = path
    
    # Split and get first folder component (the owner)
    parts = [p for p in rel_path.split("/") if p]
    if parts:
        return parts[0]  # First folder after "covers" is the owner
    return ""

def owner_from_gcs_uri(uri: str) -> str:
    """Extract Discogs folder name from gs://bucket/covers/<Owner>/<Subfolder>/file.jpg
    Returns 'Owner_Subfolder' format, joining all subdirectories with underscores.
    Example: gs://bucket/covers/Dad/Shed/image.jpg -> 'Dad_Shed'
    
    When INPUT_PREFIX includes folder structure (e.g., 'covers/Dad/Shed/'), extracts
    folder name from INPUT_PREFIX itself if file is directly under it.
    """
    if not uri:
        return ""
    p = urlparse(uri)
    path = p.path.lstrip("/")
    if path.startswith(GCS_BUCKET + "/"):
        path = path[len(GCS_BUCKET) + 1:]
    
    # Remove base prefix "covers/" to get relative path
    base_prefix = "covers/"
    if path.startswith(base_prefix):
        rel_path = path[len(base_prefix):]
    else:
        # Path doesn't start with "covers/", try to extract from INPUT_PREFIX
        if path.startswith(config.INPUT_PREFIX):
            # Extract folders from INPUT_PREFIX itself
            prefix_without_base = config.INPUT_PREFIX
            if prefix_without_base.startswith(base_prefix):
                prefix_without_base = prefix_without_base[len(base_prefix):]
            prefix_parts = [p for p in prefix_without_base.rstrip("/").split("/") if p]
            return "_".join(prefix_parts) if prefix_parts else ""
        rel_path = path
    
    # Split path into components and remove the filename (last component)
    parts = [p for p in rel_path.split("/") if p]  # Filter out empty strings
    if not parts:
        return ""
    
    # If there's only one component, it's the filename (no folders)
    if len(parts) == 1:
        # File is directly under "covers/" with no subfolders
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

