"""
Google Cloud Vision API client module.
Handles batch image annotation with chunking and rate limiting.
"""

from google.protobuf.json_format import MessageToDict
from config import VISION_SYNC_CHUNK


def chunked(seq, n):
    """Split a sequence into chunks of size n."""
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def run_vision_sync(vision_client, requests_list, uris):
    """
    Call batch_annotate_images in chunks and return response dicts
    with context.uri injected so downstream code can read it uniformly.
    """
    all_responses = []
    idx = 0
    total_batches = (len(requests_list) + VISION_SYNC_CHUNK - 1) // VISION_SYNC_CHUNK
    batch_num = 0
    
    for chunk in chunked(requests_list, VISION_SYNC_CHUNK):
        batch_num += 1
        resp = vision_client.batch_annotate_images(requests=chunk)
        print(f"Processing batch {batch_num}/{total_batches}...")
        dicts = []
        for r in resp.responses:
            try:
                # Try the current approach first
                dicts.append(MessageToDict(r._pb))
            except AttributeError:
                # Fallback if _pb doesn't exist (API change)
                dicts.append(MessageToDict(r))
        if len(dicts) != len(chunk):
            print(f"WARNING: expected {len(chunk)} responses, got {len(dicts)}")
        for d in dicts:
            if "context" not in d:
                d["context"] = {}
            d["context"]["uri"] = uris[idx] if idx < len(uris) else None
            idx += 1
        all_responses.extend(dicts)
    return all_responses

