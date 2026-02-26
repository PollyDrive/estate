"""
Utility for loading bot configuration.

Two config files:
  config/config.json   — global settings: apify, filters, llm, telegram, etc.
  config/profiles.json — array of chat profiles (each with its own stop_words)

load_config() merges them: the result dict has both global keys AND
a 'chat_profiles' key containing the profiles list.
"""

import json
import os
from pathlib import Path
from typing import Dict, Any


def _resolve_path(filename: str) -> Path:
    """Resolve config file path: repo-root/config/ or /app/config/ in Docker."""
    candidates = [
        Path(__file__).parent.parent / "config" / filename,
        Path("/app/config") / filename,
        Path("config") / filename,
    ]
    for p in candidates:
        if p.exists():
            return p
    # Return first candidate even if missing (will raise a clear FileNotFoundError)
    return candidates[0]


def load_config() -> Dict[str, Any]:
    """
    Load and merge config.json + profiles.json.

    Returns a dict with all global settings plus 'chat_profiles' list.
    """
    config_path = _resolve_path("config.json")
    profiles_path = _resolve_path("profiles.json")

    with open(config_path, "r", encoding="utf-8") as f:
        config: Dict[str, Any] = json.load(f)

    if profiles_path.exists():
        with open(profiles_path, "r", encoding="utf-8") as f:
            profiles = json.load(f)
        config["chat_profiles"] = profiles if isinstance(profiles, list) else []
    else:
        config.setdefault("chat_profiles", [])

    return config
