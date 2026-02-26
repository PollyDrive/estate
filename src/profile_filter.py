"""
Per-profile criteria check used by stage2_manual, stage3 and stage4.
Isolated here so both scripts import from one place.
"""

import logging
from typing import Tuple

logger = logging.getLogger(__name__)


def check_profile_criteria(listing: dict, profile: dict,
                           description: str = '') -> Tuple[bool, str]:
    """
    Check if a listing matches a specific chat profile's criteria.
    All required fields (bedrooms_min, price_max) are read directly from the
    profile dict — no fallback defaults. KeyError here means a misconfigured profile.

    Args:
        listing:     dict with at least: bedrooms, price_extracted, location
        profile:     chat profile dict with bedrooms_min, bedrooms_max, price_max,
                     allowed_locations, stop_locations
        description: full listing text used as fallback for location search when
                     listing['location'] is empty

    Returns (passed: bool, reason: str).
    """
    bedrooms = listing.get('bedrooms')
    price    = listing.get('price_extracted')

    # Location: prefer structured field, fall back to searching description text
    location_field = (listing.get('location') or '').strip()
    search_text    = (location_field or description or '').lower()

    br_min    = profile['bedrooms_min']
    br_max    = profile.get('bedrooms_max')   # optional — None means no upper limit
    price_max = profile['price_max']

    # ── 1. Bedrooms ───────────────────────────────────────────────────────────
    # If bedrooms is unknown (NULL) but the profile has a concrete constraint
    # (min > 1 or an explicit max), we cannot verify compliance → REJECT.
    # Only allow NULL through if the profile imposes no meaningful bedroom filter
    # (bedrooms_min == 1 and no bedrooms_max), i.e. "any bedroom count is fine".
    if bedrooms is None:
        has_constraint = br_min > 1 or br_max is not None
        if has_constraint:
            return False, f"REJECT_BEDROOMS (unknown/NULL, profile requires min={br_min} max={br_max})"
    else:
        if bedrooms < br_min:
            return False, f"REJECT_BEDROOMS ({bedrooms} < {br_min})"
        if br_max is not None and bedrooms > br_max:
            return False, f"REJECT_BEDROOMS ({bedrooms} > {br_max})"

    # ── 2. Price ──────────────────────────────────────────────────────────────
    if price is not None:
        try:
            if float(price) > float(price_max):
                return False, f"REJECT_PRICE ({price} > {price_max})"
        except (TypeError, ValueError):
            pass

    # ── 3. Allowed locations ──────────────────────────────────────────────────
    # If the profile has an allowed-list, the listing MUST mention at least one
    # allowed location somewhere in location field or description.
    # Empty search_text means we have zero location signal — REJECT, not pass-through,
    # because we cannot confirm the listing is in the right area.
    allowed = [a.lower() for a in (profile.get('allowed_locations') or [])]
    if allowed:
        if not search_text:
            return False, (
                f"REJECT_LOCATION (no location text, profile '{profile.get('name', profile['chat_id'])}' requires known area)"
            )
        if not any(a in search_text for a in allowed):
            return False, (
                f"REJECT_LOCATION (not in profile '{profile.get('name', profile['chat_id'])}' locations)"
            )

    # ── 4. Stop locations ─────────────────────────────────────────────────────
    # Short tokens (≤3 chars like "NJ", "CA", "FL") require word boundaries to
    # avoid false positives (e.g. "CA" matching inside "Canggu").
    stop_locs = [s.lower() for s in (profile.get('stop_locations') or [])]
    if stop_locs and search_text:
        import re as _re
        hit = None
        for s in stop_locs:
            if len(s) <= 3:
                if _re.search(r'\b' + _re.escape(s) + r'\b', search_text):
                    hit = s
                    break
            else:
                if s in search_text:
                    hit = s
                    break
        if hit:
            return False, f"REJECT_STOP_LOCATION ('{hit}' in profile '{profile.get('name', profile['chat_id'])}')"

    return True, "PASS"
