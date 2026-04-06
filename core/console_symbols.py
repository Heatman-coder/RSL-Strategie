import os
from typing import Dict


def build_console_symbols(stdout_encoding: str, force_ascii: bool = False) -> Dict[str, str]:
    enc = str(stdout_encoding or "").lower()
    unicode_console = (not force_ascii) and ("utf" in enc)
    emoji_mode = unicode_console and os.environ.get("RSL_EMOJI", "").strip().lower() in ("1", "true", "yes", "y")

    if emoji_mode:
        return {
            "unicode_console": unicode_console,
            "sym_market": "\U0001F4CA",
            "sym_portfolio": "\U0001F4BC",
            "sym_hold": "\U0001F7E2",
            "sym_warn": "\U0001F7E1",
            "sym_sell": "\U0001F534",
            "sym_alert": "\u26A0\uFE0F",
            "sym_delete": "\U0001F5D1",
            "sym_ok": "\u2705",
            "sym_fire": "\U0001F525",
            "sym_new": "\u2728",
            "trend_up": "\u2197",
            "trend_down": "\u2198",
            "trend_flat": "\u2192",
            "sym_divider": "\u2501"
        }

    return {
        "unicode_console": unicode_console,
        "sym_market": "\u25a3" if unicode_console else "[MARKT]",
        "sym_portfolio": "\u25a4" if unicode_console else "[PORTFOLIO]",
        "sym_hold": "\u25cf" if unicode_console else "[HOLD]",
        "sym_warn": "\u25b2" if unicode_console else "[WARN]",
        "sym_sell": "\u25bc" if unicode_console else "[SELL]",
        "sym_alert": "\u26A0" if unicode_console else "[!]",
        "sym_delete": "\u2716" if unicode_console else "[DEL]",
        "sym_ok": "\u2714" if unicode_console else "[OK]",
        "sym_fire": "\u2738" if unicode_console else "[*]",
        "sym_new": "*" if unicode_console else "[NEU]",
        "trend_up": "\u2197" if unicode_console else "UP",
        "trend_down": "\u2198" if unicode_console else "DN",
        "trend_flat": "\u2192" if unicode_console else "->",
        "sym_divider": "\u2500" if unicode_console else "-"
    }
