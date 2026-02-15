from copy import deepcopy
from typing import Any, Dict, Optional


DEFAULT_APIFY_PROXY_CONFIG: Dict[str, Any] = {
    "useApifyProxy": False,
    "apifyProxyGroups": ["RESIDENTIAL"],
}


def build_apify_proxy_config(
    configured_proxy: Optional[Dict[str, Any]] = None,
    *,
    country: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build a normalized Apify proxy config.
    Uses configured proxy when provided, otherwise a shared default.
    """
    proxy_cfg = (
        deepcopy(configured_proxy)
        if isinstance(configured_proxy, dict)
        else deepcopy(DEFAULT_APIFY_PROXY_CONFIG)
    )
    if country:
        proxy_cfg["proxyCountry"] = country
    return proxy_cfg

