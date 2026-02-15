"""
Facebook Marketplace Stage 2 scraper using actor qFR6mjgdwPouKLDvE.
Keeps Cheerio scraper untouched and provides an alternative pipeline.
"""

import logging
from typing import Any, Dict, List, Optional

from apify_client import ApifyClient
from apify_proxy import build_apify_proxy_config

logger = logging.getLogger(__name__)


class FacebookMarketplaceQfrScraper:
    """Stage 2 full-detail scraper via Apify actor qFR6mjgdwPouKLDvE."""

    ACTOR_ID = "qFR6mjgdwPouKLDvE"

    def __init__(self, api_key: str, config: Dict):
        self.client = ApifyClient(api_key)
        self.config = config or {}
        self.cfg = (self.config.get("marketplace_qfr") or {})
        logger.info("FacebookMarketplaceQfrScraper initialized (actor=%s)", self.ACTOR_ID)

    def scrape_full_details(self, listing_urls: List[str], max_stage2_items: int = 50) -> List[Dict[str, Any]]:
        if not listing_urls:
            return []

        urls = listing_urls[:max_stage2_items]
        if len(listing_urls) > max_stage2_items:
            logger.warning("[QFR] Limiting from %s to %s items", len(listing_urls), max_stage2_items)

        run_input = self._build_actor_input(urls)
        logger.info("[QFR] Starting scrape for %s URLs", len(urls))
        logger.info("[QFR] Actor input keys: %s", list(run_input.keys()))

        run = self.client.actor(self.ACTOR_ID).call(run_input=run_input)
        logger.info("[QFR] Actor run ID: %s", run.get("id"))
        logger.info("[QFR] Actor status: %s", run.get("status"))

        dataset_id = run["defaultDatasetId"]
        items = list(self.client.dataset(dataset_id).iterate_items())
        logger.info("[QFR] Fetched %s items from dataset", len(items))

        normalized: List[Dict[str, Any]] = []
        for idx, raw in enumerate(items):
            listing = self.normalize_listing(raw)
            if listing:
                # Best-effort URL injection by order if actor omits URL.
                if not listing.get("listing_url") and idx < len(urls):
                    listing["listing_url"] = urls[idx]
                normalized.append(listing)

        logger.info("[QFR] Normalized %s listings", len(normalized))
        return normalized

    def _build_actor_input(self, urls: List[str]) -> Dict[str, Any]:
        """
        Based on actor documentation summary and common Apify Marketplace patterns.
        """
        run_input: Dict[str, Any] = {
            "startUrls": [{"url": u} for u in urls],
            "maxItems": len(urls),
            "fetchDetails": True,
            "getNewItems": True,
            "proxyConfiguration": build_apify_proxy_config(
                self.cfg.get("proxy"),
                country="Indonesia",
            ),
        }
        # Optional knobs (only if configured)
        if self.cfg.get("sortBy"):
            run_input["sortBy"] = self.cfg["sortBy"]
        if self.cfg.get("minPrice") is not None:
            run_input["minPrice"] = self.cfg["minPrice"]
        if self.cfg.get("maxPrice") is not None:
            run_input["maxPrice"] = self.cfg["maxPrice"]
        if self.cfg.get("maxConcurrency") is not None:
            run_input["maxConcurrency"] = self.cfg["maxConcurrency"]
        return run_input

    def normalize_listing(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(raw, dict):
            return None

        fb_id = self._extract_fb_id(raw)
        title = self._s(raw, "marketplace_listing_title") or self._s(raw, "headline") or self._s(raw, "title")
        description = (
            self._s(raw, "marketplace_listing_description")
            or self._s(raw, "description")
            or self._s(raw, "fullDescription")
        )
        listing_url = self._s(raw, "listingUrl") or self._s(raw, "url") or self._s(raw, "permalink")
        price = self._extract_price(raw)
        location = self._extract_location(raw)

        if not fb_id and not listing_url:
            return None
        if not title and not description:
            return None

        return {
            "fb_id": fb_id or "",
            "title": title or "",
            "description": description or "",
            "listing_url": listing_url or "",
            "price": price or "",
            "location": location or "",
            "raw_data": raw,
        }

    def _extract_fb_id(self, raw: Dict[str, Any]) -> Optional[str]:
        direct = self._s(raw, "id") or self._s(raw, "listingId") or self._s(raw, "postId")
        if direct:
            return direct

        url = self._s(raw, "listingUrl") or self._s(raw, "url") or ""
        if "/item/" in url:
            return url.split("/item/")[-1].split("?")[0].split("/")[0]
        return None

    def _extract_price(self, raw: Dict[str, Any]) -> str:
        p = raw.get("listing_price")
        if isinstance(p, dict):
            return self._s(p, "formatted_amount_zeros_stripped") or self._s(p, "formatted_amount") or ""
        return self._s(raw, "price")

    def _extract_location(self, raw: Dict[str, Any]) -> str:
        loc = raw.get("location")
        if isinstance(loc, dict):
            rg = loc.get("reverse_geocode") or {}
            if isinstance(rg, dict):
                return self._s(rg, "city") or self._s(rg, "display_name") or self._s(rg, "state")
            return self._s(loc, "text") or self._s(loc, "name")
        return self._s(raw, "location_text") or self._s(raw, "location")

    @staticmethod
    def _s(obj: Dict[str, Any], key: str) -> str:
        v = obj.get(key) if isinstance(obj, dict) else None
        if v is None:
            return ""
        if isinstance(v, dict):
            for k in ("text", "label", "value", "name"):
                if v.get(k):
                    return str(v.get(k))
            return ""
        if isinstance(v, list):
            return ", ".join(str(x) for x in v if x)
        return str(v)

