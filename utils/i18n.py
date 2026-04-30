"""
i18n — Internationalisation helper for the Anchor Automation Dashboard.

Usage
-----
    from utils.i18n import t, get_lang, set_lang, lang_selector

    # In sidebar:
    lang_selector()          # renders EN/ID toggle, persists to session state

    # Anywhere:
    t("nav.analytics")       # → "Analytics" (EN) or "Analitik" (ID)
    t("kpi.merchants", n=42) # → "42 Merchants Tracked" (supports **kwargs)
"""

import json
import os
import streamlit as st
from functools import lru_cache
from typing import Any

_DIR = os.path.dirname(os.path.abspath(__file__))
_TRANSLATIONS_PATH = os.path.join(os.path.dirname(_DIR), "locales", "translations.json")

# ── Translation loader ────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def _load_translations() -> dict:
    """Load and cache the translations JSON file.  Called once per process."""
    if not os.path.exists(_TRANSLATIONS_PATH):
        return {"en": {}, "id": {}}
    with open(_TRANSLATIONS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def reload_translations() -> None:
    """Force-reload translations (useful during development)."""
    _load_translations.cache_clear()


# ── Language state helpers ────────────────────────────────────────────────────

def get_lang() -> str:
    """Return current language code from session state (default 'en')."""
    return st.session_state.get("lang", "en")


def set_lang(lang_code: str) -> None:
    """Persist language choice into session state."""
    st.session_state["lang"] = lang_code


def lang_selector(key: str = "lang_selector") -> None:
    """Render a compact language toggle in the sidebar."""
    current = get_lang()
    options = ["EN", "ID"]
    idx = 0 if current == "en" else 1
    choice = st.selectbox(
        "🌐 Language",
        options,
        index=idx,
        key=key,
        label_visibility="collapsed",
    )
    new_lang = "en" if choice == "EN" else "id"
    if new_lang != current:
        set_lang(new_lang)
        st.rerun()


# ── Core translation function ────────────────────────────────────────────────

def t(key: str, **kwargs: Any) -> str:
    """
    Look up a translation string by dot-notation key.

    Parameters
    ----------
    key : str
        Dot-separated path into the translations dict, e.g. "nav.analytics".
    **kwargs
        Format parameters injected via str.format(), e.g. t("kpi.x", n=5).

    Returns
    -------
    str
        The translated string, or the key wrapped in « » as a visible fallback.
    """
    lang = get_lang()
    data = _load_translations()
    namespace = data.get(lang, data.get("en", {}))

    # Dot-notation traversal
    node = namespace
    for part in key.split("."):
        if isinstance(node, dict):
            node = node.get(part)
        else:
            node = None
            break

    if node is None:
        # Fallback: try English if current lang is missing the key
        if lang != "en":
            en_ns = data.get("en", {})
            en_node = en_ns
            for part in key.split("."):
                if isinstance(en_node, dict):
                    en_node = en_node.get(part)
                else:
                    en_node = None
                    break
            if en_node is not None and isinstance(en_node, str):
                try:
                    return en_node.format(**kwargs) if kwargs else en_node
                except (KeyError, IndexError):
                    return en_node

        # Ultimate fallback — visible debug marker
        return f"\u00ab{key}\u00bb"

    if isinstance(node, str):
        try:
            return node.format(**kwargs) if kwargs else node
        except (KeyError, IndexError):
            return node

    # If node is a dict (partial key), return fallback
    return f"\u00ab{key}\u00bb"
