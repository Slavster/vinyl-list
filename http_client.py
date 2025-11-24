"""
HTTP client with retry logic for API calls.
Handles rate limiting, exponential backoff, and error handling.
"""

import time
import random
import requests
from config import (
    DISCOGS_APP_NAME, DISCOGS_APP_VERSION, DISCOGS_CONTACT, 
    DISCOGS_APP_URL, DISCOGS_TOKEN
)


def discogs_headers():
    """Generate Discogs API headers with user-agent and authentication."""
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
    """HTTP POST with retry logic."""
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
    """HTTP PUT with retry logic."""
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

