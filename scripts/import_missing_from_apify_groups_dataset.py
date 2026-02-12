#!/usr/bin/env python3
"""
Backfill items from Apify Facebook Groups Scraper dataset into DB.

Why:
- Sometimes the actor run produces far more items than our app imported
  (timeouts / manual runs / different actor inputs).
- This script pulls items directly from Apify dataset and stores *everything* that is missing:
  - items that pass current Stage1 filters -> `listings`
  - items that don't pass -> `listing_non_relevant` with `move_reason`

Default behavior:
- Fetches the most recent run for actor `apify/facebook-groups-scraper`
  (can be overridden via --run-id or --dataset-id).

Usage:
  python3 scripts/import_missing_from_apify_groups_dataset.py
  python3 scripts/import_missing_from_apify_groups_dataset.py --run-id <RUN_ID>
  python3 scripts/import_missing_from_apify_groups_dataset.py --dataset-id <DATASET_ID>
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

from dotenv import load_dotenv
from apify_client import ApifyClient

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from database import Database
from property_parser import PropertyParser


logger = logging.getLogger(__name__)


ACTOR_ID = "apify/facebook-groups-scraper"


def _extract_group_id_from_url(url: str) -> Optional[str]:
    if not url or "/groups/" not in url:
        return None
    try:
        parts = url.split("/groups/")
        if len(parts) < 2:
            return None
        return parts[1].split("/")[0].split("?")[0] or None
    except Exception:
        return None


def _extract_fb_id(raw_item: Dict[str, Any]) -> Optional[str]:
    """
    Best-effort extraction of stable fb_id used in our DB.
    We prefer actor-provided postId; otherwise try URL patterns.
    """
    fb_id = raw_item.get("postId") or raw_item.get("post_id") or raw_item.get("id")
    if fb_id:
        return str(fb_id)

    url = raw_item.get("url") or raw_item.get("link") or ""
    if not url:
        return None

    # Common group post URL patterns:
    # - /permalink/1234567890/
    # - /posts/1234567890
    import re

    m = re.search(r"/permalink/(\d+)", url)
    if m:
        return f"group_post_{m.group(1)}"
    m = re.search(r"/posts/(\d+)", url)
    if m:
        return f"group_post_{m.group(1)}"

    # permalink.php?story_fbid=...&id=...
    m = re.search(r"story_fbid=(\d+)", url)
    if m:
        return f"group_post_{m.group(1)}"

    return None


def _normalize_item(raw_item: Dict[str, Any]) -> Optional[Dict[str, str]]:
    url = (raw_item.get("url") or raw_item.get("link") or "").strip()
    if not url:
        return None

    # Actor typically uses `text` for post body; sometimes `title` exists.
    title = (raw_item.get("title") or "").strip()
    text = (raw_item.get("text") or raw_item.get("postText") or "").strip()

    # We require some text content to be useful.
    description = "\n".join([p for p in [title, text] if p]).strip()
    if not description:
        return None

    fb_id = _extract_fb_id(raw_item)
    if not fb_id:
        return None

    group_id = raw_item.get("groupId") or raw_item.get("group_id") or _extract_group_id_from_url(url)
    group_id = str(group_id) if group_id else ""

    return {
        "fb_id": fb_id,
        "group_id": group_id,
        "listing_url": url,
        "description": description,
    }


def _iter_dataset_items(client: ApifyClient, dataset_id: str) -> Iterable[Dict[str, Any]]:
    # iterate_items() yields all items with pagination handled by SDK.
    return client.dataset(dataset_id).iterate_items()


def _resolve_dataset_id(client: ApifyClient, run_id: Optional[str], dataset_id: Optional[str]) -> Tuple[str, str]:
    """
    Returns (run_id, dataset_id). If run_id is not provided, uses most recent run.
    """
    if dataset_id:
        return (run_id or "", dataset_id)

    if not run_id:
        runs = list(client.actor(ACTOR_ID).runs().list(limit=5).items)
        if not runs:
            raise RuntimeError(f"No runs found for actor {ACTOR_ID}")
        run_id = runs[0].get("id")
        if not run_id:
            raise RuntimeError("Could not determine latest run id")

    run = client.run(run_id).get()
    if not run:
        raise RuntimeError(f"Run not found: {run_id}")

    dataset_id = run.get("defaultDatasetId")
    if not dataset_id:
        raise RuntimeError(f"Run {run_id} has no defaultDatasetId")

    return (run_id, dataset_id)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    load_dotenv()

    parser = argparse.ArgumentParser(description="Import missing items from Apify groups dataset")
    parser.add_argument("--run-id", default=None, help="Apify run id to import from")
    parser.add_argument("--dataset-id", default=None, help="Apify dataset id to import from (overrides run id)")
    parser.add_argument(
        "--config",
        default="config/config.json",
        help="Path to config.json (default: config/config.json)",
    )
    args = parser.parse_args()

    api_key = os.getenv("APIFY_API_KEY")
    if not api_key:
        raise RuntimeError("Missing APIFY_API_KEY in environment")

    client = ApifyClient(api_key)
    resolved_run_id, resolved_dataset_id = _resolve_dataset_id(client, args.run_id, args.dataset_id)
    logger.info(f"Using actor: {ACTOR_ID}")
    if resolved_run_id:
        logger.info(f"Run ID: {resolved_run_id}")
    logger.info(f"Dataset ID: {resolved_dataset_id}")

    # Load config + initialize parser for Stage1-style filtering.
    import json

    config_path = args.config
    if not os.path.exists(config_path):
        # docker path fallback
        config_path = "/app/config/config.json"
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    stage1_parser = PropertyParser(config)
    criterias = config.get("criterias", {})
    stop_words = config.get("filters", {}).get("stop_words", [])
    stop_locations = config.get("filters", {}).get("stop_locations", [])

    # Preload existing fb_ids to avoid per-row SELECT.
    with Database() as db:
        db.cursor.execute("SELECT fb_id FROM listings")
        existing_in_listings = {row[0] for row in db.cursor.fetchall()}
        db.cursor.execute("SELECT fb_id FROM listing_non_relevant")
        existing_in_non_rel = {row[0] for row in db.cursor.fetchall()}

    total_items = 0
    normalized_ok = 0
    skipped_empty = 0
    skipped_existing = 0
    saved_to_listings = 0
    saved_to_non_rel = 0
    save_errors = 0

    with Database() as db:
        for raw in _iter_dataset_items(client, resolved_dataset_id):
            total_items += 1

            # Skip actor error items
            if isinstance(raw, dict) and ("error" in raw):
                skipped_empty += 1
                continue

            item = _normalize_item(raw)
            if not item:
                skipped_empty += 1
                continue

            normalized_ok += 1
            fb_id = item["fb_id"]

            if fb_id in existing_in_listings or fb_id in existing_in_non_rel:
                skipped_existing += 1
                continue

            # Apply Stage1-like filters to decide where to store the item.
            description = item["description"]
            params = stage1_parser.parse(description)
            passed, reason = stage1_parser.matches_criteria(params, criterias, stage=1)

            if passed and description:
                desc_lower = description.lower()
                for sw in stop_words:
                    if sw.lower() in desc_lower:
                        passed = False
                        reason = f"Stop word in description: {sw}"
                        break

            # Groups run usually has empty structured location; keep the check for completeness.
            if passed:
                loc = ""
                loc_lower = loc.lower() if loc else ""
                for sl in stop_locations:
                    if sl.lower().replace("in ", "").strip() and sl.lower().replace("in ", "").strip() in loc_lower:
                        passed = False
                        reason = f"Stop location found: {sl}"
                        break

            try:
                if passed:
                    was_added = db.add_listing_from_stage1(
                        fb_id=fb_id,
                        title="",
                        price="",
                        location="",
                        listing_url=item["listing_url"],
                        source="facebook_group",
                        group_id=item["group_id"] or None,
                        description=item["description"],
                    )
                    if was_added:
                        saved_to_listings += 1
                        existing_in_listings.add(fb_id)
                else:
                    db.cursor.execute(
                        """
                        INSERT INTO listing_non_relevant
                          (fb_id, title, price, location, listing_url, description, source, group_id, move_reason)
                        VALUES
                          (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (fb_id) DO NOTHING
                        """,
                        (
                            fb_id,
                            "",
                            "",
                            "",
                            item["listing_url"],
                            item["description"],
                            "facebook_group",
                            item["group_id"] or None,
                            f"STAGE1_FILTERED: {reason}",
                        ),
                    )
                    db.conn.commit()
                    saved_to_non_rel += 1
                    existing_in_non_rel.add(fb_id)
            except Exception as e:
                save_errors += 1
                logger.warning(f"Could not save fb_id={fb_id}: {e}")

    logger.info("IMPORT COMPLETE")
    logger.info(f"Total dataset items: {total_items}")
    logger.info(f"Normalized OK: {normalized_ok}")
    logger.info(f"Skipped (empty/error/unusable): {skipped_empty}")
    logger.info(f"Skipped (already in DB): {skipped_existing}")
    logger.info(f"Saved to listings: {saved_to_listings}")
    logger.info(f"Saved to listing_non_relevant: {saved_to_non_rel}")
    logger.info(f"Save errors: {save_errors}")


if __name__ == "__main__":
    main()

