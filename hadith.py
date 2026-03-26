"""
hadith.py — Hadith API Client
==============================
A Python client for searching and retrieving hadith from 6 major collections,
with support for Arabic, English, and French.

Source: fawazahmed0 Hadith API (https://github.com/fawazahmed0/hadith-api)
License: Free to use, no API key required.

Collections:
    bukhari   — Sahih al-Bukhari
    muslim    — Sahih Muslim
    abudawud  — Sunan Abu Dawud
    tirmidhi  — Jami' at-Tirmidhi
    ibnmajah  — Sunan Ibn Majah
    nasai     — Sunan an-Nasa'i

Usage:
    from hadith import HadithClient

    client = HadithClient()

    # Fetch by reference
    result = client.get_by_number("bukhari", 1)

    # Search by text (English)
    results = client.search("bukhari", "prayer", lang="en")

    # Search by text (Arabic)
    results = client.search("bukhari", "الصلاة", lang="ar")
"""

import json
import re
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from typing import Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BASE_URL = "https://cdn.jsdelivr.net/gh/fawazahmed0/hadith-api@1/editions"

_EDITIONS = {
    "bukhari":  {"name": "Sahih al-Bukhari",   "ar": "ara-bukhari",  "en": "eng-bukhari",  "fr": "fra-bukhari"},
    "muslim":   {"name": "Sahih Muslim",        "ar": "ara-muslim",   "en": "eng-muslim",   "fr": "fra-muslim"},
    "abudawud": {"name": "Sunan Abu Dawud",     "ar": "ara-abudawud", "en": "eng-abudawud", "fr": "fra-abudawud"},
    "tirmidhi": {"name": "Jami' at-Tirmidhi",  "ar": "ara-tirmidhi", "en": "eng-tirmidhi", "fr": "fra-tirmidhi"},
    "ibnmajah": {"name": "Sunan Ibn Majah",     "ar": "ara-ibnmajah", "en": "eng-ibnmajah", "fr": "fra-ibnmajah"},
    "nasai":    {"name": "Sunan an-Nasa'i",     "ar": "ara-nasai",    "en": "eng-nasai",    "fr": "fra-nasai"},
}

COLLECTIONS = list(_EDITIONS.keys())
LANGUAGES = ("ar", "en", "fr")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class HadithText:
    """Multilingual text for a single hadith."""
    ar: Optional[str] = None   # Arabic
    en: Optional[str] = None   # English
    fr: Optional[str] = None   # French


@dataclass
class Hadith:
    """A single hadith entry."""
    collection: str              # e.g. "bukhari"
    collection_name: str         # e.g. "Sahih al-Bukhari"
    number: int                  # hadith number
    text: HadithText = field(default_factory=HadithText)

    def __repr__(self) -> str:
        preview = (self.text.en or self.text.ar or self.text.fr or "")[:80]
        return f"<Hadith {self.collection_name} #{self.number}: {preview!r}...>"

    def to_dict(self) -> dict:
        return {
            "collection": self.collection,
            "collection_name": self.collection_name,
            "number": self.number,
            "text": {
                "ar": self.text.ar,
                "en": self.text.en,
                "fr": self.text.fr,
            },
        }

    def format(self, langs: tuple = ("ar", "en", "fr"), width: int = 72) -> str:
        """Return a formatted human-readable string."""
        sep = "─" * width
        lines = [
            sep,
            f"  {self.collection_name}  —  Hadith #{self.number}",
            sep,
        ]
        labels = {"ar": "Arabic / عربي", "en": "English", "fr": "Français"}
        for lang in langs:
            txt = getattr(self.text, lang)
            if txt:
                lines.append(f"\n[{labels[lang]}]")
                lines.append(txt)
        lines.append(sep)
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class HadithClient:
    """
    Client for the fawazahmed0 Hadith API.

    Parameters
    ----------
    cache : bool
        Whether to cache full-collection data in memory (default True).
        Caching makes repeated text searches fast after the first load.
    timeout : int
        HTTP request timeout in seconds (default 30).

    Examples
    --------
    >>> client = HadithClient()

    # Single hadith by number
    >>> h = client.get_by_number("bukhari", 1)
    >>> print(h.text.en)

    # Search by keyword (English)
    >>> results = client.search("muslim", "charity", lang="en", limit=5)
    >>> for h in results:
    ...     print(h.number, h.text.en[:60])

    # Search by keyword (Arabic)
    >>> results = client.search("bukhari", "الصلاة", lang="ar", limit=10)
    """

    def __init__(self, cache: bool = True, timeout: int = 30):
        self._cache_enabled = cache
        self._timeout = timeout
        self._collection_cache: dict[str, list[dict]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_by_number(
        self,
        collection: str,
        number: int,
        langs: tuple = ("ar", "en", "fr"),
    ) -> Hadith:
        """
        Fetch a specific hadith by its number.

        Parameters
        ----------
        collection : str
            One of: bukhari, muslim, abudawud, tirmidhi, ibnmajah, nasai
        number : int
            The hadith number (1-based).
        langs : tuple
            Languages to fetch. Any subset of ("ar", "en", "fr").

        Returns
        -------
        Hadith

        Raises
        ------
        ValueError  — unknown collection or invalid number
        LookupError — hadith not found
        RuntimeError — network or API error
        """
        self._validate_collection(collection)
        if number < 1:
            raise ValueError(f"Hadith number must be >= 1, got {number}")

        edition_info = _EDITIONS[collection]
        text = HadithText()

        for lang in langs:
            if lang not in LANGUAGES:
                continue
            edition_key = edition_info[lang]
            url = f"{_BASE_URL}/{edition_key}/{number}.json"
            try:
                data = self._fetch_json(url)
                hadiths = data.get("hadiths", [])
                if hadiths:
                    setattr(text, lang, hadiths[0].get("text"))
            except LookupError:
                pass  # this language might not have this hadith number
            except RuntimeError as e:
                # French is often missing — don't raise for optional lang
                if lang == "fr":
                    pass
                else:
                    raise

        if not any([text.ar, text.en, text.fr]):
            raise LookupError(
                f"Hadith #{number} not found in '{edition_info['name']}'."
            )

        return Hadith(
            collection=collection,
            collection_name=edition_info["name"],
            number=number,
            text=text,
        )

    def search(
        self,
        collection: str,
        query: str,
        lang: str = "en",
        langs: tuple = ("ar", "en", "fr"),
        limit: int = 20,
        case_sensitive: bool = False,
    ) -> list[Hadith]:
        """
        Search hadith text by keyword or phrase.

        Parameters
        ----------
        collection : str
            One of: bukhari, muslim, abudawud, tirmidhi, ibnmajah, nasai
        query : str
            Search term (word or phrase). Arabic, French, or English.
        lang : str
            Language to search in — "en" (default), "ar", or "fr".
            Note: French ("fr") bulk downloads are not available for all
            collections in the API. When lang="fr", the client automatically
            downloads the English bulk collection, fetches the French text for
            each match individually, then filters by your query against the
            French text. This is slower but fully accurate.
        langs : tuple
            Languages to include in the returned Hadith objects.
        limit : int
            Maximum number of results to return (default 20).
        case_sensitive : bool
            Whether the search is case-sensitive (default False).

        Returns
        -------
        list[Hadith]

        Notes
        -----
        The first call for a collection downloads the full dataset (~a few MB).
        Subsequent searches on the same collection use the in-memory cache.
        """
        self._validate_collection(collection)
        if lang not in LANGUAGES:
            raise ValueError(f"lang must be one of {LANGUAGES}, got {lang!r}")

        # --- French search: special path ---
        # French bulk collection JSONs are not reliably available in the API.
        # Strategy: load English bulk collection to get all hadith numbers,
        # fetch French text for each one individually, then filter by query.
        if lang == "fr":
            return self._search_french(
                collection, query, langs=langs, limit=limit,
                case_sensitive=case_sensitive,
            )

        edition_key = _EDITIONS[collection][lang]
        all_hadiths = self._load_collection(edition_key)

        # Match
        flags = 0 if case_sensitive else re.IGNORECASE
        try:
            pattern = re.compile(re.escape(query), flags)
        except re.error:
            pattern = re.compile(query, flags)

        matches = [
            h for h in all_hadiths
            if h.get("text") and pattern.search(h["text"])
        ][:limit]

        if not matches:
            return []

        results = []
        for h in matches:
            number = h.get("hadithnumber") or h.get("number")
            text = HadithText()

            # We already have one language's text
            setattr(text, lang, h["text"])

            # Fetch other requested languages
            for other_lang in langs:
                if other_lang == lang or other_lang not in LANGUAGES:
                    continue
                other_edition = _EDITIONS[collection][other_lang]
                url = f"{_BASE_URL}/{other_edition}/{number}.json"
                try:
                    data = self._fetch_json(url)
                    hadiths = data.get("hadiths", [])
                    if hadiths:
                        setattr(text, other_lang, hadiths[0].get("text"))
                except (LookupError, RuntimeError):
                    pass

            results.append(Hadith(
                collection=collection,
                collection_name=_EDITIONS[collection]["name"],
                number=number,
                text=text,
            ))

        return results

    def search_all_collections(
        self,
        query: str,
        lang: str = "en",
        langs: tuple = ("ar", "en", "fr"),
        limit_per_collection: int = 5,
    ) -> dict[str, list[Hadith]]:
        """
        Search across all 6 collections at once.

        Parameters
        ----------
        query : str
            Search keyword or phrase.
        lang : str
            Language to search in ("en", "ar", "fr").
        langs : tuple
            Languages to include in results.
        limit_per_collection : int
            Max results per collection (default 5).

        Returns
        -------
        dict mapping collection key → list[Hadith]

        Example
        -------
        >>> all_results = client.search_all_collections("paradise", lang="en")
        >>> for col, hadiths in all_results.items():
        ...     print(col, len(hadiths))
        """
        results = {}
        for col in COLLECTIONS:
            try:
                hits = self.search(col, query, lang=lang, langs=langs, limit=limit_per_collection)
                results[col] = hits
            except Exception as e:
                results[col] = []
        return results

    def list_collections(self) -> list[dict]:
        """Return info about all available collections."""
        return [
            {"key": k, "name": v["name"], "editions": {l: v[l] for l in LANGUAGES}}
            for k, v in _EDITIONS.items()
        ]

    def clear_cache(self):
        """Clear the in-memory collection cache."""
        self._collection_cache.clear()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _search_french(
        self,
        collection: str,
        query: str,
        langs: tuple = ("ar", "en", "fr"),
        limit: int = 20,
        case_sensitive: bool = False,
    ) -> list[Hadith]:
        """
        French search strategy:
          1. Load the English bulk collection to get all hadith numbers.
          2. For each hadith, fetch the French text individually.
          3. Filter by query against the French text.
          4. Return up to `limit` results with all requested languages.

        This is slower than bulk-collection search because it makes one HTTP
        request per hadith for the French text. Results are cached per hadith
        so repeated searches on the same collection are fast after first run.
        """
        flags = 0 if case_sensitive else re.IGNORECASE
        try:
            pattern = re.compile(re.escape(query), flags)
        except re.error:
            pattern = re.compile(query, flags)

        fr_edition = _EDITIONS[collection]["fr"]
        en_edition = _EDITIONS[collection]["en"]

        # Use English bulk to get the full list of hadith numbers
        all_hadiths = self._load_collection(en_edition)

        # Per-collection French text cache
        fr_cache_key = f"fr_texts:{collection}"
        if fr_cache_key not in self._collection_cache:
            self._collection_cache[fr_cache_key] = {}
        fr_texts: dict = self._collection_cache[fr_cache_key]

        results = []
        for h in all_hadiths:
            if len(results) >= limit:
                break

            number = h.get("hadithnumber") or h.get("number")

            # Fetch French text (use cache if available)
            if number not in fr_texts:
                url = f"{_BASE_URL}/{fr_edition}/{number}.json"
                try:
                    data = self._fetch_json(url)
                    hadiths = data.get("hadiths", [])
                    fr_texts[number] = hadiths[0].get("text") if hadiths else None
                except (LookupError, RuntimeError):
                    fr_texts[number] = None

            fr_text = fr_texts.get(number)
            if not fr_text or not pattern.search(fr_text):
                continue

            # Match found — build Hadith with all requested languages
            text = HadithText(fr=fr_text)

            for other_lang in langs:
                if other_lang == "fr":
                    continue
                if other_lang == "en":
                    text.en = h.get("text")  # already in bulk data
                    continue
                if other_lang == "ar":
                    ar_edition = _EDITIONS[collection]["ar"]
                    ar_url = f"{_BASE_URL}/{ar_edition}/{number}.json"
                    try:
                        data = self._fetch_json(ar_url)
                        hadiths = data.get("hadiths", [])
                        if hadiths:
                            text.ar = hadiths[0].get("text")
                    except (LookupError, RuntimeError):
                        pass

            results.append(Hadith(
                collection=collection,
                collection_name=_EDITIONS[collection]["name"],
                number=number,
                text=text,
            ))

        return results

    def _validate_collection(self, collection: str):
        if collection not in _EDITIONS:
            raise ValueError(
                f"Unknown collection {collection!r}. "
                f"Choose from: {', '.join(COLLECTIONS)}"
            )

    def _fetch_json(self, url: str) -> dict:
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise LookupError(f"Not found: {url}")
            raise RuntimeError(f"HTTP {e.code} fetching {url}")
        except urllib.error.URLError as e:
            raise RuntimeError(f"Network error: {e.reason}")

    def _load_collection(self, edition_key: str) -> list[dict]:
        """Download and cache the full hadith list for an edition."""
        if self._cache_enabled and edition_key in self._collection_cache:
            return self._collection_cache[edition_key]

        url = f"{_BASE_URL}/{edition_key}.json"
        data = self._fetch_json(url)

        # Normalize structure (some editions nest under chapters)
        raw = data.get("hadiths") or []
        if not raw and "chapters" in data:
            for ch in data["chapters"]:
                raw.extend(ch.get("hadiths", []))

        normalized = []
        for h in raw:
            num = h.get("hadithnumber") or h.get("number")
            text = h.get("text") or h.get("body") or ""
            if num and text:
                normalized.append({"hadithnumber": num, "text": text})

        if self._cache_enabled:
            self._collection_cache[edition_key] = normalized

        return normalized


# ---------------------------------------------------------------------------
# Quick CLI — python hadith.py
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    client = HadithClient()

    print("=" * 60)
    print("  Hadith Client — Quick Demo")
    print("=" * 60)

    # 1. Fetch by reference
    print("\n[1] Fetching Bukhari #1 (by reference)...\n")
    try:
        h = client.get_by_number("bukhari", 1)
        print(h.format(langs=("ar", "en")))
    except Exception as e:
        print(f"  Error: {e}")

    # 2. Text search
    query = sys.argv[1] if len(sys.argv) > 1 else "intention"
    print(f"\n[2] Searching Bukhari for '{query}' (English)...\n")
    try:
        results = client.search("bukhari", query, lang="en", langs=("en", "ar"), limit=3)
        if results:
            for r in results:
                print(r.format(langs=("en", "ar")))
                print()
        else:
            print("  No results found.")
    except Exception as e:
        print(f"  Error: {e}")
