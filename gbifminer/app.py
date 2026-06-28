"""
GBIF Predation/Parasitism Observation Miner
Flask backend — designed for Hugging Face Spaces deployment.

Mines GBIF occurrence records for interaction evidence in free-text fields:
  occurrenceRemarks, fieldNotes, eventRemarks, behavior, habitat,
  dynamicProperties, verbatimLocality, samplingProtocol, locationRemarks,
  associatedTaxa, associatedOccurrences
"""

import csv
import io
import json
import queue
import re
import threading
import time
import uuid
from datetime import datetime

import pandas as pd
import requests
from flask import Flask, Response, jsonify, request, send_file, stream_with_context
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Allow GitHub-hosted frontend to hit HF-hosted backend

# ─── In-memory job store ──────────────────────────────────────────────────────
jobs: dict[str, dict] = {}

# ─── GBIF API constants ───────────────────────────────────────────────────────

GBIF_OCCURRENCE_URL  = "https://api.gbif.org/v1/occurrence/search"
GBIF_SPECIES_URL     = "https://api.gbif.org/v1/species"
GBIF_SUGGEST_URL     = "https://api.gbif.org/v1/species/suggest"
GBIF_MATCH_URL       = "https://api.gbif.org/v1/species/match"

# GBIF text fields that may contain interaction evidence (DwC terms)
TEXT_FIELDS = [
    "occurrenceRemarks",
    "fieldNotes",
    "eventRemarks",
    "behavior",
    "habitat",
    "dynamicProperties",
    "verbatimLocality",
    "samplingProtocol",
    "locationRemarks",
    "associatedTaxa",
    "associatedOccurrences",
    "verbatimCoordinates",   # sometimes carries notes
    "taxonRemarks",
]

# ─── Interaction terms ────────────────────────────────────────────────────────

INTERACTION_TERMS = [
    # Predation cluster
    "predat",        # predation, predator, predatory
    "prey",
    "eaten",
    "kill",
    # Parasitism cluster
    "parasit",       # parasite, parasitoid, parasitism, parasitized
    "cuckoo",
    "cleptoparasit",
    "kleptoparasit",
    "inquiline",
    "koinobiont",
    "idiobiont",
    # Ectoparasites / phoresy
    "mite",
    "phoret",
    "ectoparasit",
    "endoparasit",
    # Attack / capture
    "attack",
    "captured",
    "caught",
    "stolen",
    # Context terms
    "host",
    "nest",
    "brood",
    "larval",
    # Specific enemy common names
    "beewolf",
    "bee wolf",
    "velvet ant",
    "crab spider",
    "ambush bug",
    # Behavioural evidence
    "paralyz",
    "stung",
    "pounced",
    "grabbed",
    "ambush",
    "hunt",
]

# Target keywords to confirm the record involves the target taxon as victim/host/prey
TARGET_KEYWORDS = [
    "melissodes",
    "eucerini",
    "eucera",
    "long-horned bee",
    "longhorned bee",
    "tetraloniella",
    "cemolobus",
]

# Self-referential patterns (target is the SUBJECT, not the victim)
TARGET_SELF_PATTERNS = [
    "nesting aggregation", "nest hole", "nest site", "nesting site",
    "nesting in", "burrowing", "ground nest", "soil nest", "emerged from nest",
    "nesting here", "nesting bee", "making nest", "dug", "digging",
    "foraging on", "foraging for", "collecting pollen", "pollen on",
    "visiting flower", "on flower", "on goldenrod", "on sunflower",
    "males patrol", "male patrol", "long antennae",
    "new brood", "brood back", "brood this year",
    "encourage predator", "beneficial predator", "orchard", "flower strip",
    "mite on", "mites on",
]

# Phrases that confirm the enemy IS acting on the target
ENEMY_CONFIRMATION_PATTERNS = [
    "preying on", "prey on", "preys on", "prey bee",
    "attacked by", "captured by", "killed by", "eaten by",
    "parasitize", "parasitised", "parasitized", "parasitism",
    "cleptoparasit", "kleptoparasit",
    "host bee", "host melissodes", "host: melissodes",
    "triepeolus", "holonomada", "philanthus", "phymata",
    "ambush bug", "beewolf", "bee wolf",
    "crab spider", "velvet ant",
    "preying", "pounced", "overpower",
    "stolen pollen", "stolen provisions",
    "paralyzed", "paralysed", "stung the bee",
]

# Terms too self-referential to use in direct-taxon scans
TARGET_SKIP_TERMS = {"host", "nest", "brood", "larval", "phoret"}

# ─── GBIF helpers ─────────────────────────────────────────────────────────────

CONNECT_TIMEOUT = 10
READ_TIMEOUT    = 60
MAX_RETRIES     = 3
BACKOFF_BASE    = 2
GBIF_PAGE_SIZE  = 300   # GBIF max per request


def _gbif_get(url: str, params: dict, attempt_log=None) -> dict | None:
    """Robust GBIF GET with retry/back-off. Returns parsed JSON or None."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params,
                             timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
                             headers={"User-Agent": "GBIF-Interaction-Miner/1.0"})
            if r.status_code == 429:
                wait = BACKOFF_BASE ** attempt * 5
                if attempt_log:
                    attempt_log(f"    rate-limited (429), waiting {wait}s…", "warn")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError):
            pass
        except Exception:
            pass
        if attempt < MAX_RETRIES:
            time.sleep(BACKOFF_BASE ** attempt)
    return None


def gbif_occurrence_page(params: dict, log_fn=None) -> tuple[list, int]:
    """
    Fetch one page of GBIF occurrences.
    Returns (results, endOfRecords_or_total).
    """
    data = _gbif_get(GBIF_OCCURRENCE_URL, params, log_fn)
    if data is None:
        return [], 0
    return data.get("results", []), data.get("count", 0)


def extract_text_fields(occ: dict) -> dict[str, str]:
    """Extract all relevant text fields from a GBIF occurrence record."""
    out = {}
    for f in TEXT_FIELDS:
        v = occ.get(f)
        if v and isinstance(v, str) and v.strip():
            out[f] = v.strip()
    return out


def build_combined_text(occ: dict, text_fields: dict[str, str]) -> str:
    """Build a single lowercase combined string from all text fields."""
    parts = list(text_fields.values())
    # Also include species name, vernacularName if present
    sn = occ.get("species") or occ.get("scientificName") or ""
    vn = occ.get("vernacularName") or ""
    if sn:
        parts.append(sn)
    if vn:
        parts.append(vn)
    return " ".join(parts).lower()


def text_mentions_target(combined: str) -> bool:
    """Return True if the text mentions the target taxon (as victim/host/prey)."""
    for kw in TARGET_KEYWORDS:
        idx = combined.find(kw)
        if idx == -1:
            continue
        after = combined[idx + len(kw):]
        if after.startswith("tle"):   # "bee" + "tle" = "beetle"
            continue
        return True
    # Long antennae + "bee" heuristic
    if "long antennae" in combined and re.search(r"\bbee\b", combined):
        return True
    return False


def text_has_interaction(combined: str) -> bool:
    """Return True if the text contains any interaction term."""
    if any(term in combined for term in INTERACTION_TERMS):
        return True
    if any(p in combined for p in ENEMY_CONFIRMATION_PATTERNS):
        return True
    return False


def _is_self_referential(combined: str) -> bool:
    """Return True if the observation is about the target's OWN behaviour."""
    has_ofv_prey_signal = any(p in combined for p in ENEMY_CONFIRMATION_PATTERNS)
    if has_ofv_prey_signal:
        return False   # clear enemy signal overrides
    return any(p in combined for p in TARGET_SELF_PATTERNS)


def parse_occurrence(occ: dict, taxon_name: str, term: str,
                     require_target_in_text: bool = False,
                     is_direct_taxon_query: bool = False) -> dict | None:
    """
    Parse a GBIF occurrence into a record dict.
    Returns None if it should be excluded.
    """
    text_fields = extract_text_fields(occ)
    combined    = build_combined_text(occ, text_fields)

    # Must have SOME text to evaluate
    if not combined.strip():
        return None

    # ── All-records mode: must mention target in text ─────────────────────────
    if require_target_in_text:
        if not text_mentions_target(combined):
            return None
        if not text_has_interaction(combined):
            return None

    # ── Direct-taxon mode: check at least one interaction term appears ─────────
    elif is_direct_taxon_query:
        if not text_has_interaction(combined):
            return None
        # Self-referential filter
        if _is_self_referential(combined):
            return None

    # ── No text evidence at all ───────────────────────────────────────────────
    else:
        if not text_has_interaction(combined):
            return None

    # ── Build clean record ────────────────────────────────────────────────────
    occ_id   = str(occ.get("key") or occ.get("gbifID") or "")
    sci_name = (occ.get("scientificName") or
                occ.get("species") or
                occ.get("genus") or "Unknown")

    # Which field(s) had the hit
    hit_fields = [f for f, v in text_fields.items() if
                  any(t in v.lower() for t in INTERACTION_TERMS) or
                  any(p in v.lower() for p in ENEMY_CONFIRMATION_PATTERNS) or
                  (require_target_in_text and text_mentions_target(v.lower()))]

    # Best remarks text for preview (prefer occurrenceRemarks > fieldNotes > others)
    preview_text = ""
    for f in ["occurrenceRemarks", "fieldNotes", "eventRemarks", "behavior",
              "habitat", "dynamicProperties", "associatedTaxa"]:
        if f in text_fields:
            preview_text = text_fields[f][:600]
            break

    # Coordinates
    lat  = occ.get("decimalLatitude")
    lon  = occ.get("decimalLongitude")
    coords = f"{lat:.4f}, {lon:.4f}" if (lat is not None and lon is not None) else ""

    # Country / locality
    country   = occ.get("country") or occ.get("countryCode") or ""
    locality  = occ.get("locality") or occ.get("verbatimLocality") or ""
    place     = ", ".join(p for p in [locality, country] if p) or coords

    # Dataset
    dataset = occ.get("datasetName") or occ.get("collectionCode") or ""

    # Basis of record
    basis = occ.get("basisOfRecord") or ""

    # Associated taxa field (DwC)
    assoc_taxa = occ.get("associatedTaxa") or ""

    return {
        "id": occ_id,
        "taxon_queried": taxon_name,
        "keyword": term,
        "url": f"https://www.gbif.org/occurrence/{occ_id}",
        "observed_taxon": sci_name,
        "user": occ.get("recordedBy") or occ.get("institutionCode") or "",
        "created_at": str(occ.get("eventDate") or occ.get("year") or "")[:10],
        "place": place,
        "country": country,
        "dataset": dataset,
        "basis_of_record": basis,
        "occurrence_remarks": (text_fields.get("occurrenceRemarks") or "")[:500],
        "field_notes": (text_fields.get("fieldNotes") or "")[:500],
        "behavior": (text_fields.get("behavior") or "")[:300],
        "associated_taxa": assoc_taxa[:300],
        "hit_fields": " | ".join(hit_fields),
        "preview_text": preview_text,
        "coords": coords,
        "review": "",
    }


# ─── GBIF taxon lookup ────────────────────────────────────────────────────────

def gbif_taxon_suggest(name: str) -> list[dict]:
    """Return GBIF species suggestions for a name string."""
    data = _gbif_get(GBIF_SUGGEST_URL, {"q": name, "limit": 10})
    return data if isinstance(data, list) else []


def gbif_taxon_match(name: str) -> dict | None:
    """Exact-match a name against GBIF backbone."""
    data = _gbif_get(GBIF_MATCH_URL, {"name": name, "verbose": False})
    return data if data and data.get("usageKey") else None


def gbif_common_name(taxon_key: int, log_fn=None) -> str | None:
    """
    Fetch vernacular names for a GBIF taxon key.
    Returns the first English name found, or None.
    """
    data = _gbif_get(f"{GBIF_SPECIES_URL}/{taxon_key}/vernacularNames",
                     {"limit": 20}, log_fn)
    if not data:
        return None
    for vn in data.get("results", []):
        lang = (vn.get("language") or "").lower()
        name = (vn.get("vernacularName") or "").strip()
        if name and lang in ("eng", "en", "english", ""):
            return name.lower()
    # No English — try any language
    results = data.get("results", [])
    if results:
        n = (results[0].get("vernacularName") or "").strip()
        if n:
            return n.lower()
    return None


def gbif_parent_common_name(taxon_key: int, log_fn=None) -> tuple[str | None, str | None]:
    """
    Walk up the GBIF taxonomic tree looking for a common name.
    Returns (common_name, rank_found_at).
    """
    CLIMB_RANKS = ("genus", "tribe", "subfamily", "family", "order")
    data = _gbif_get(f"{GBIF_SPECIES_URL}/{taxon_key}", {}, log_fn)
    if not data:
        return None, None

    ancestors_keys = []
    for rank in CLIMB_RANKS:
        key = data.get(f"{rank}Key")
        if key and key != taxon_key:
            ancestors_keys.append((rank, key))

    for rank, key in ancestors_keys:
        cn = gbif_common_name(key, log_fn)
        if cn:
            return cn, rank

    return None, None


# ─── Sweep functions ──────────────────────────────────────────────────────────

def _sweep_gbif_pages(params: dict, log_fn, job: dict, cap: int = 10_000) -> list:
    """
    Page through GBIF occurrences for the given params, up to cap records.
    Returns list of raw occurrence dicts.
    """
    all_results = []
    offset = 0
    total = None

    while True:
        if job.get("cancel"):
            break
        p = {**params, "limit": GBIF_PAGE_SIZE, "offset": offset}
        data = _gbif_get(GBIF_OCCURRENCE_URL, p, log_fn)
        if not data:
            log_fn("    page fetch failed (timeout/error)", "warn")
            break

        results = data.get("results", [])
        if total is None:
            total = data.get("count", 0)
            log_fn(f"    total matches: {total:,}")

        if not results:
            break

        all_results.extend(results)

        if data.get("endOfRecords", False) or len(all_results) >= min(total, cap):
            break

        offset += len(results)
        if offset >= cap:
            log_fn(f"    cap of {cap:,} reached — stopping pagination", "warn")
            break

        time.sleep(0.4)

    return all_results


def sweep_taxon_term(taxon_key: int, term: str, log_fn, job: dict,
                     target_name: str) -> list:
    """
    Stream 1: search GBIF occurrences for a specific taxon_key + term in text.
    Uses GBIF's full-text 'q' parameter.
    """
    params = {
        "taxonKey": taxon_key,
        "q": term,
        "hasCoordinate": "false",   # include records without coords
    }
    log_fn(f"    GBIF taxonKey={taxon_key} q='{term}'")
    return _sweep_gbif_pages(params, log_fn, job)


def sweep_all_records_term(term: str, target_kw: str, log_fn, job: dict) -> list:
    """
    Stream 2: search ALL of GBIF with compound q='<term> <target_kw>'.
    No taxon filter — catches any organism's occurrence that co-mentions both.
    """
    q = f"{term} {target_kw}"
    params = {"q": q}
    log_fn(f"    q='{q}'")
    return _sweep_gbif_pages(params, log_fn, job, cap=5_000)


# ─── Workers ──────────────────────────────────────────────────────────────────

def mine_worker(job_id: str, taxon_key: int, taxon_name: str, terms: list[str],
                signal_done: bool = True):
    """
    Stream 1: Direct target-taxon scan.
    Searches GBIF for occurrences of taxon_key that contain interaction terms
    in their free-text fields. Applies self-referential false positive filtering.
    """
    job = jobs[job_id]
    total_terms = len(terms)
    job["total_terms"] = total_terms
    job["status"] = "running"

    def log(msg: str, level: str = "info"):
        job["log_queue"].put({"msg": msg, "level": level,
                              "ts": datetime.utcnow().isoformat()})

    log(f"▶ Stream 1 — Direct {taxon_name} scan: {total_terms} keywords", "milestone")

    for i, term in enumerate(terms, 1):
        if job.get("cancel"):
            log("⛔ Job cancelled by user.", "warn")
            break

        if term in TARGET_SKIP_TERMS:
            log(f"[{i}/{total_terms}] Skipping '{term}' (self-referential for {taxon_name})")
            job["done_terms"] = i
            continue

        log(f"[{i}/{total_terms}] Scanning GBIF for '{term}' in {taxon_name} occurrences…")
        results = sweep_taxon_term(taxon_key, term, log, job, taxon_name)

        new_count = 0
        for occ in results:
            oid = str(occ.get("key") or occ.get("gbifID") or "")
            if not oid:
                continue
            with job["_seen_lock"]:
                if oid in job["_seen_ids"]:
                    continue
                job["_seen_ids"].add(oid)

            record = parse_occurrence(occ, taxon_name, term,
                                      is_direct_taxon_query=True)
            if record:
                job["rows"].append(record)
                new_count += 1

        if new_count:
            log(f"  ✓ +{new_count} records (total: {len(job['rows'])})", "checkpoint")
        else:
            log(f"  — 0 new records for '{term}'")

        job["done_terms"] = i
        time.sleep(0.5)

    job["keyword_scan_done"] = True
    log(f"✓ Stream 1 complete. {len(job['rows'])} interaction records so far.", "milestone")

    if signal_done:
        with job["_threads_lock"]:
            job["_threads_done"] += 1
            if job["_threads_done"] >= job["_threads_total"]:
                job["status"] = "done"
                job["log_queue"].put({"status": "done", "total": len(job["rows"])})


def mine_all_records_worker(job_id: str, target_name: str, terms: list[str]):
    """
    Stream 2: All-Records sweep across all of GBIF.

    For each (interaction_term, target_keyword) pair, queries GBIF with
    q='<term> <target_kw>' — no taxon filter. The combined query is tiny
    (GBIF ANDs both words server-side), then we locally verify the record
    mentions the target taxon as victim/host and actually has interaction evidence.

    Catches records like: a wasp specimen with occurrenceRemarks
    "collected while preying on Melissodes", filed under Philanthus.
    """
    job = jobs[job_id]
    job["status"] = "running"

    def log(msg: str, level: str = "info"):
        job["log_queue"].put({"msg": msg, "level": level,
                              "ts": datetime.utcnow().isoformat()})

    n_pairs = len(terms) * len(TARGET_KEYWORDS)
    log(f"▶ Stream 2 — Cross-product GBIF sweep (all taxa)", "milestone")
    log(f"  {len(terms)} terms × {len(TARGET_KEYWORDS)} target keywords = {n_pairs} queries", "info")
    log("  Checks: occurrenceRemarks, fieldNotes, behavior, associatedTaxa, …", "info")

    total_new = 0

    for term_i, term in enumerate(terms, 1):
        if job.get("cancel"):
            log("⛔ Job cancelled by user.", "warn")
            break

        log(f"[{term_i}/{len(terms)}] Sweeping all GBIF for '{term}' × {len(TARGET_KEYWORDS)} targets…")
        term_new = 0

        for kw in TARGET_KEYWORDS:
            if job.get("cancel"):
                break

            results = sweep_all_records_term(term, kw, log, job)

            for occ in results:
                oid = str(occ.get("key") or occ.get("gbifID") or "")
                if not oid:
                    continue
                with job["_seen_lock"]:
                    if oid in job["_seen_ids"]:
                        continue
                    job["_seen_ids"].add(oid)

                # Use observed taxon name for labelling
                obs_taxon = (occ.get("scientificName") or
                             occ.get("species") or "Unknown")

                record = parse_occurrence(occ, obs_taxon, f"{term}+{kw}",
                                          require_target_in_text=True)
                if record:
                    job["rows"].append(record)
                    term_new += 1
                    total_new += 1

            time.sleep(0.3)

        job["done_terms"] = term_i
        log(f"  ✓ '{term}' done: {term_new} new (grand total: {len(job['rows'])})",
            "checkpoint" if term_new else "info")

    log(f"✓ Stream 2 complete: {total_new} interaction records found.", "milestone")
    job["keyword_scan_done"] = True
    with job["_threads_lock"]:
        job["_threads_done"] += 1
        if job["_threads_done"] >= job["_threads_total"]:
            job["status"] = "done"
            job["log_queue"].put({"status": "done", "total": len(job["rows"])})


def mine_predator_list_worker(job_id: str, taxon_key: int, taxon_name: str,
                               raw_list: str, resume_from_index: int = 0,
                               skip_keyword_scan: bool = False):
    """
    Stream 3 (optional): three-phase predator-list workflow.

    PHASE 1 — For every taxon in the predator list, look it up on GBIF to get
              a usageKey and try to resolve a vernacular/common name.
              Falls back up the rank ladder: genus → family → order.

    PHASE 2 — Build unified keyword set:
                INTERACTION_TERMS
              + resolved common names
              + raw scientific names (lowercased)

    PHASE 3 — Run mine_worker with the full keyword set.
    """
    job = jobs[job_id]

    def log(msg, level="info"):
        job["log_queue"].put({"msg": msg, "level": level,
                              "ts": datetime.utcnow().isoformat()})

    names = [n.strip() for n in raw_list.replace(",", "\n").replace(";", "\n")
             .splitlines() if n.strip()]
    total_names = len(names)

    if resume_from_index > 0:
        log(f"▶ Resuming predator list at index {resume_from_index}/{total_names} "
            f"({total_names - resume_from_index} remaining)", "milestone")
        names = names[resume_from_index:]
        start_i = resume_from_index + 1
    else:
        start_i = 1

    # ── PHASE 1: resolve common names via GBIF ───────────────────────────────
    log(f"▶ Phase 1/3 — Querying GBIF common names for {len(names)} predator taxa…",
        "milestone")

    keyword_map: dict[str, list[dict]] = {}
    sci_names: list[str] = []
    unresolved_count = 0

    for i, name in enumerate(names, start_i):
        if job.get("cancel"):
            log("⛔ Job cancelled (Phase 1).", "warn")
            break
        job["predator_index_done"] = i - 1

        sci_names.append(name.lower())

        # Try GBIF name match first (fast, exact)
        match = gbif_taxon_match(name)
        usage_key = None
        matched_name = None

        if match and match.get("usageKey"):
            usage_key = match["usageKey"]
            matched_name = match.get("canonicalName") or match.get("scientificName") or name
        else:
            # Fall back to suggest
            suggestions = gbif_taxon_suggest(name)
            if suggestions:
                best = suggestions[0]
                usage_key = best.get("key")
                matched_name = best.get("canonicalName") or best.get("scientificName") or name

        if not usage_key:
            log(f"  [{i}/{total_names}] '{name}' — no GBIF match; scientific name kept",
                "warn")
            unresolved_count += 1
            time.sleep(0.4)
            continue

        # Try direct common name
        cn = gbif_common_name(usage_key, log)
        if cn:
            log(f"  [{i}/{total_names}] {matched_name} → \"{cn}\"")
            keyword_map.setdefault(cn, []).append({
                "key": usage_key, "name": matched_name,
                "source_rank": "direct", "source_taxon": matched_name,
            })
        else:
            # Walk up the tree
            parent_cn, parent_rank = gbif_parent_common_name(usage_key, log)
            if parent_cn:
                log(f"  [{i}/{total_names}] {matched_name} — no direct name; "
                    f"using {parent_rank} → \"{parent_cn}\"")
                keyword_map.setdefault(parent_cn, []).append({
                    "key": usage_key, "name": matched_name,
                    "source_rank": parent_rank, "source_taxon": matched_name,
                })
            else:
                log(f"  [{i}/{total_names}] {matched_name} — no common name at any rank; "
                    f"scientific name kept", "warn")
                unresolved_count += 1

        time.sleep(0.4)

    job["predator_index_done"] = resume_from_index + len(names)
    resolved_count = len(names) - unresolved_count
    log(f"✓ Phase 1/3 complete: {resolved_count}/{len(names)} resolved to "
        f"{len(keyword_map)} unique common-name keywords. "
        f"{len(sci_names)} scientific names collected.", "milestone")

    # ── PHASE 2: build keyword list ──────────────────────────────────────────
    if job.get("cancel"):
        log("⛔ Cancelled before Phase 2.", "warn")
        job["keyword_scan_done"] = True
        with job["_threads_lock"]:
            job["_threads_done"] += 1
            if job["_threads_done"] >= job["_threads_total"]:
                job["status"] = "done"
                job["log_queue"].put({"status": "done", "total": len(job["rows"])})
        return

    seen_kw: set[str] = set()
    all_keywords: list[str] = []
    for kw in list(INTERACTION_TERMS) + list(keyword_map.keys()) + sci_names:
        kw_lower = kw.lower()
        if kw_lower in seen_kw or kw_lower in TARGET_SKIP_TERMS:
            continue
        seen_kw.add(kw_lower)
        all_keywords.append(kw_lower)

    log(f"▶ Phase 2/3 — Keyword set: {len(INTERACTION_TERMS)} preset + "
        f"{len(keyword_map)} common names + {len(sci_names)} sci names "
        f"= {len(all_keywords)} unique keywords", "milestone")

    # ── PHASE 3: scan ────────────────────────────────────────────────────────
    log(f"▶ Phase 3/3 — Scanning {taxon_name} GBIF records with "
        f"{len(all_keywords)} keywords…", "milestone")

    if not job.get("cancel") and not skip_keyword_scan and all_keywords:
        mine_worker(job_id, taxon_key, taxon_name, all_keywords)
    elif not all_keywords:
        log("✓ No keywords — scan skipped.", "milestone")
        job["keyword_scan_done"] = True
        with job["_threads_lock"]:
            job["_threads_done"] += 1
            if job["_threads_done"] >= job["_threads_total"]:
                job["status"] = "done"
                job["log_queue"].put({"status": "done", "total": len(job["rows"])})
    else:
        log("⛔ Cancelled or scan skipped.", "warn")
        job["keyword_scan_done"] = True
        with job["_threads_lock"]:
            job["_threads_done"] += 1
            if job["_threads_done"] >= job["_threads_total"]:
                job["status"] = "done"
                job["log_queue"].put({"status": "done", "total": len(job["rows"])})


# ─── Flask routes ─────────────────────────────────────────────────────────────

@app.route("/api/mine/<job_id>/stream")
def mine_stream(job_id):
    """SSE stream for live log + progress updates."""
    if job_id not in jobs:
        return jsonify({"error": "job not found"}), 404

    def generate():
        job = jobs[job_id]
        q = job["log_queue"]
        while True:
            try:
                item = q.get(timeout=20)
            except queue.Empty:
                hb = json.dumps({
                    "heartbeat": True,
                    "done_terms": job.get("done_terms", 0),
                    "total_terms": job.get("total_terms", 1),
                    "total_rows": len(job["rows"]),
                    "status": job["status"],
                })
                yield f"data: {hb}\n\n"
                if (job["status"] == "done" and
                        job.get("_threads_done", 0) >= job.get("_threads_total", 1)):
                    break
                continue

            payload = {
                **item,
                "done_terms": job.get("done_terms", 0),
                "total_terms": job.get("total_terms", 1),
                "total_rows": len(job["rows"]),
                "status": job["status"],
            }
            yield f"data: {json.dumps(payload)}\n\n"

            if (item.get("status") == "done" and
                    job.get("_threads_done", 0) >= job.get("_threads_total", 1)):
                break

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/mine/<job_id>/status")
def mine_status(job_id):
    if job_id not in jobs:
        return jsonify({"error": "not found"}), 404
    job = jobs[job_id]
    return jsonify({
        "status": job["status"],
        "done_terms": job.get("done_terms", 0),
        "total_terms": job.get("total_terms", 1),
        "total_rows": len(job["rows"]),
    })


@app.route("/api/mine/<job_id>/rows")
def mine_rows(job_id):
    if job_id not in jobs:
        return jsonify({"error": "not found"}), 404
    return jsonify(jobs[job_id]["rows"])


@app.route("/api/mine/<job_id>/review", methods=["POST"])
def mine_review(job_id):
    if job_id not in jobs:
        return jsonify({"error": "not found"}), 404
    data = request.get_json(force=True)
    obs_id = str(data.get("id"))
    verdict = data.get("review")
    for row in jobs[job_id]["rows"]:
        if str(row["id"]) == obs_id:
            row["review"] = verdict
            return jsonify({"ok": True})
    return jsonify({"error": "record not found"}), 404


@app.route("/api/mine/<job_id>/download")
def mine_download(job_id):
    if job_id not in jobs:
        return jsonify({"error": "not found"}), 404
    rows = jobs[job_id]["rows"]
    if not rows:
        return jsonify({"error": "no data"}), 400
    df = pd.DataFrame(rows)
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=gbif_interactions_{job_id}.csv"},
    )


@app.route("/api/mine/<job_id>/checkpoint")
def mine_checkpoint(job_id):
    if job_id not in jobs:
        return jsonify({"error": "not found"}), 404
    job = jobs[job_id]
    return jsonify({
        "job_id": job_id,
        "taxon_key": job.get("taxon_key"),
        "taxon_name": job.get("taxon_name"),
        "predator_list": job.get("predator_list", ""),
        "status": job["status"],
        "total_rows": len(job["rows"]),
        "done_terms": job.get("done_terms", 0),
        "total_terms": job.get("total_terms", 0),
        "rows": job["rows"],
        "predator_index_done": job.get("predator_index_done", 0),
        "keyword_scan_done": job.get("keyword_scan_done", False),
        "saved_at": datetime.utcnow().isoformat(),
    })


@app.route("/api/mine/<job_id>/cancel", methods=["POST"])
def mine_cancel(job_id):
    if job_id not in jobs:
        return jsonify({"error": "not found"}), 404
    jobs[job_id]["cancel"] = True
    return jsonify({"ok": True})


@app.route("/api/taxon/search")
def taxon_search():
    """Proxy GBIF species suggest autocomplete."""
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify([])
    try:
        data = gbif_taxon_suggest(q)
        results = []
        for t in (data or []):
            results.append({
                "key": t.get("key"),
                "name": t.get("canonicalName") or t.get("scientificName") or "",
                "rank": (t.get("rank") or "").lower(),
                "common_name": t.get("vernacularName") or "",
                "kingdom": t.get("kingdom") or "",
                "family": t.get("family") or "",
            })
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/mine/start", methods=["POST"])
def mine_start():
    data = request.get_json(force=True)
    taxon_key  = int(data.get("taxon_key", 0) or data.get("taxon_id", 0))
    taxon_name = data.get("taxon_name", "Unknown")
    predator_list_raw = data.get("predator_list", "")

    resume_from_index = int(data.get("resume_from_index", 0))
    preloaded_rows    = data.get("preloaded_rows", [])
    skip_keyword_scan = bool(data.get("skip_keyword_scan", False))
    all_records_mode  = bool(data.get("all_records_mode", False))
    custom_terms      = data.get("custom_terms", [])

    if not taxon_key:
        return jsonify({"error": "taxon_key required"}), 400

    terms = custom_terms if custom_terms else list(INTERACTION_TERMS)

    job_id = str(uuid.uuid4())[:8]
    has_pred_list = bool(predator_list_raw.strip()) and not all_records_mode
    threads_needed = 1

    jobs[job_id] = {
        "status": "queued",
        "rows": list(preloaded_rows),
        "log_queue": queue.Queue(),
        "total_terms": len(terms),
        "done_terms": 0,
        "taxon_key": taxon_key,
        "taxon_name": taxon_name,
        "predator_list": predator_list_raw,
        "all_records_mode": all_records_mode,
        "started_at": datetime.utcnow().isoformat(),
        "cancel": False,
        "predator_index_done": resume_from_index,
        "keyword_scan_done": skip_keyword_scan,
        "_threads_total": threads_needed,
        "_threads_done": 0,
        "_threads_lock": threading.Lock(),
        "_seen_ids": set(str(r["id"]) for r in preloaded_rows),
        "_seen_lock": threading.Lock(),
    }

    if has_pred_list:
        thread = threading.Thread(
            target=mine_predator_list_worker,
            args=(job_id, taxon_key, taxon_name, predator_list_raw,
                  resume_from_index, skip_keyword_scan),
            daemon=True,
        )
        thread.start()
    elif not skip_keyword_scan:
        if all_records_mode:
            def _sequential_streams():
                mine_worker(job_id, taxon_key, taxon_name, terms, signal_done=False)
                mine_all_records_worker(job_id, taxon_name, terms)
            thread = threading.Thread(target=_sequential_streams, daemon=True)
        else:
            thread = threading.Thread(
                target=mine_worker,
                args=(job_id, taxon_key, taxon_name, terms),
                daemon=True,
            )
        thread.start()
    else:
        jobs[job_id]["status"] = "done"

    return jsonify({"job_id": job_id, "taxon_name": taxon_name})


@app.route("/health")
def health():
    return jsonify({"status": "ok", "source": "gbif"})


@app.route("/ping")
def ping():
    return jsonify({"status": "ok", "message": "pong"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7860, debug=False)