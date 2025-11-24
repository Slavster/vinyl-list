"""
Vision API result caching module.
Handles loading and saving Vision API results to avoid redundant API calls.
"""

import os
import json

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

