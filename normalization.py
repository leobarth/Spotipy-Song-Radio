"""Shared normalization helpers for artist and track matching."""

import re
import unicodedata


_VERSION_SUFFIX_PATTERNS = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"\s*-\s*\d{4}\s*remaster(?:ed)?.*$",
        r"\s*-\s*remaster(?:ed)?.*$",
        r"\s*\(\s*\d{4}\s*remaster(?:ed)?.*?\)",
        r"\s*\(remaster(?:ed)?.*?\)",
        r"\s*-\s*live.*$",
        r"\s*\(live.*?\)",
        r"\s*-\s*radio edit.*$",
        r"\s*\(radio edit\)",
        r"\s*-\s*single version.*$",
        r"\s*\(single version\)",
        r"\s*-\s*mono.*$",
        r"\s*-\s*stereo.*$",
        r"\s*-\s*deluxe.*$",
        r"\s*\(deluxe.*?\)",
        r"\s*-\s*bonus track.*$",
        r"\s*\(bonus track\)",
        r"\s*-\s*acoustic.*$",
        r"\s*\(acoustic.*?\)",
        r"\s*-\s*instrumental.*$",
        r"\s*\(instrumental\)",
        r"\s*\(explicit\)",
        r"\s*\(clean\)",
        r"\s*\(feat\..*?\)",
        r"\s*\(with .*?\)",
        r"\s*-\s*[^-]*\b(?:edit|mix|version|edition|anniversary|extended|demo|reissue|expanded|alternate)\b.*$",
        r"\s*\([^()]*\b(?:edit|mix|version|edition|anniversary|extended|demo|reissue|expanded|alternate)\b[^()]*\)",
    )
)


def normalize_name(value):
    """Return a Unicode-normalized, case-insensitive key."""
    if not isinstance(value, str):
        return ""
    return unicodedata.normalize("NFKC", value).casefold().strip()


def _normalize_tokens(value):
    """Normalize text and replace punctuation with single spaces."""
    normalized = unicodedata.normalize("NFKD", value).casefold()
    chars = []
    for char in normalized:
        if unicodedata.category(char).startswith("M"):
            continue
        chars.append(char if char.isalnum() else " ")
    return " ".join("".join(chars).split())


def normalize_artist_name(name):
    """Return a punctuation-insensitive artist matching key."""
    return _normalize_tokens(name) if isinstance(name, str) else ""


def normalize_track_title(title):
    """Return a track-title key with common version suffixes removed."""
    if not isinstance(title, str):
        return ""

    normalized = unicodedata.normalize("NFKC", title).strip()
    previous = None
    while normalized != previous:
        previous = normalized
        for pattern in _VERSION_SUFFIX_PATTERNS:
            normalized = pattern.sub("", normalized).strip()

    return _normalize_tokens(normalized)