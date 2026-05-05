import os
import time

import streamlit as st

# ── Session-based rate limiter ─────────────────────────────────────────────────

def check_rate_limit(key: str, max_calls: int, window_seconds: int) -> bool:
    """Return True if the call is allowed, False if the session is over-limit."""
    now = time.time()
    state_key = f"_rl_{key}"
    if state_key not in st.session_state:
        st.session_state[state_key] = []
    st.session_state[state_key] = [
        t for t in st.session_state[state_key]
        if now - t < window_seconds
    ]
    if len(st.session_state[state_key]) >= max_calls:
        return False
    st.session_state[state_key].append(now)
    return True


def enforce_rate_limit(
    key: str,
    max_calls: int,
    window_seconds: int,
    label: str = "requests",
) -> None:
    """Stop the page with an error message if the session rate limit is exceeded."""
    if not check_rate_limit(key, max_calls, window_seconds):
        st.error(
            f"Too many {label} — please wait {window_seconds} seconds before trying again.",
            icon="🛑",
        )
        st.stop()


# ── Global pipeline cooldown (file-backed, cross-session) ─────────────────────

_COOLDOWN_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    ".pipeline_cooldown",
)
PIPELINE_COOLDOWN_SECONDS = 90


def is_pipeline_cooling_down() -> tuple[bool, float]:
    """Return (is_blocked, seconds_remaining)."""
    if not os.path.exists(_COOLDOWN_FILE):
        return False, 0.0
    try:
        with open(_COOLDOWN_FILE) as f:
            last_run = float(f.read().strip())
        remaining = PIPELINE_COOLDOWN_SECONDS - (time.time() - last_run)
        return remaining > 0, max(0.0, remaining)
    except Exception:
        return False, 0.0


def set_pipeline_cooldown() -> None:
    """Record the current time as the last pipeline run. Call before starting the run."""
    with open(_COOLDOWN_FILE, "w") as f:
        f.write(str(time.time()))
