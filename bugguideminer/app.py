"""
BugGuide Interaction Miner
Flask backend — scrapes BugGuide.net for predation/parasitism records
involving a target taxon (default: Melissodes).

Scraping strategy:
  - requests + BeautifulSoup first (fast, no browser overhead)
  - Playwright headless Chromium fallback per-page if requests returns 403/empty

Three streams (run sequentially):
  Stream 1 — Direct target taxon scan: paginate the target's BugGuide node,
             check image record remarks/notes for interaction keywords.
  Stream 2 — Predator list mode: resolve each user-supplied taxon to a BugGuide
             node, paginate its image records, check for target mentions.
  Stream 3 — All Records: BugGuide sitewide image search for target name,
             returns every image record mentioning it regardless of taxon.
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
from bs4 import BeautifulSoup
from flask import Flask, Response, jsonify, request, send_file, stream_with_context
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# ── In-memory job store ────────────────────────────────────────────────────────
jobs: dict[str, dict] = {}

# ── BugGuide constants ─────────────────────────────────────────────────────────
BUGGUIDE_BASE = "https://bugguide.net"
BUGGUIDE_NODE  = f"{BUGGUIDE_BASE}/node/view"
BUGGUIDE_SEARCH = f"{BUGGUIDE_BASE}/index.php"

# Realistic browser headers — BugGuide 403s on default Python UA
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://bugguide.net/",
}

REQUEST_TIMEOUT = (10, 30)
RETRY_DELAYS    = [2, 5, 10]   # seconds between retries

# ── Interaction keywords (default) ─────────────────────────────────────────────
DEFAULT_INTERACTION_TERMS = [
    "predat", "prey", "eaten", "eating", "kill", "killed",
    "parasit", "cleptoparasit", "kleptoparasit", "cuckoo",
    "mite", "phoret", "ectoparasit",
    "attack", "captured", "caught", "stolen",
    "host", "nest", "brood", "larval",
    "beewolf", "bee wolf", "velvet ant", "crab spider", "ambush bug",
    "paralyz", "stung", "pounced", "grabbed", "ambush", "hunt",
    "inquiline", "koinobiont", "idiobiont",
]

# ── Target keywords (what to look for in predator-side records) ────────────────
DEFAULT_TARGET_KEYWORDS = [
    "melissodes",
    "eucerini",
    "eucera",
    "long-horned bee",
    "longhorned bee",
    "tetraloniella",
    "cemolobus",
]

# ── BugGuide image record fields to extract ────────────────────────────────────
# BugGuide image pages have: Remarks, Life Stage, Behavior, Size, and sometimes
# a "Sex" field and free-text caption. We extract all of them.
EXTRACT_FIELDS = ["remarks", "life stage", "behavior", "size", "sex", "caption", "body"]


# ══════════════════════════════════════════════════════════════════════════════
# LOW-LEVEL FETCH  (requests first, Playwright fallback)
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_html_requests(url: str) -> str | None:
    """
    Attempt to fetch a URL with requests.
    Returns HTML string on success, None on failure.
    """
    for delay in [0] + RETRY_DELAYS:
        if delay:
            time.sleep(delay)
        try:
            r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200 and len(r.text) > 500:
                return r.text
            if r.status_code == 429:
                time.sleep(30)
                continue
            if r.status_code in (403, 503):
                return None   # signal to try Playwright
        except requests.exceptions.RequestException:
            continue
    return None


def _fetch_html_playwright(url: str) -> str | None:
    """
    Fallback: fetch URL with headless Chromium via Playwright.
    Returns HTML string on success, None on failure.
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent=HEADERS["User-Agent"],
                extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            )
            page = ctx.new_page()
            try:
                page.goto(url, timeout=30_000, wait_until="domcontentloaded")
                page.wait_for_load_state("networkidle", timeout=10_000)
            except PWTimeout:
                pass
            html = page.content()
            browser.close()
            return html if len(html) > 500 else None
    except Exception:
        return None


def fetch_html(url: str, log_fn=None) -> str | None:
    """
    Fetch URL: requests first, Playwright fallback.
    Logs which method was used if log_fn provided.
    """
    html = _fetch_html_requests(url)
    if html:
        return html
    if log_fn:
        log_fn(f"    requests failed for {url} — trying Playwright…", "warn")
    html = _fetch_html_playwright(url)
    if html and log_fn:
        log_fn(f"    Playwright succeeded for {url}", "info")
    return html


# ══════════════════════════════════════════════════════════════════════════════
# BUGGUIDE TAXON RESOLUTION
# ══════════════════════════════════════════════════════════════════════════════

def resolve_bugguide_node(taxon_name: str, log_fn=None) -> tuple[str | None, str | None]:
    """
    Search BugGuide for a taxon name and return (node_id, canonical_name).
    Uses the BugGuide taxon search page.
    Returns (None, None) if not found.
    """
    url = f"{BUGGUIDE_SEARCH}?q=search&ts=taxon&search={requests.utils.quote(taxon_name)}"
    html = fetch_html(url, log_fn)
    if not html:
        return None, None

    soup = BeautifulSoup(html, "html.parser")

    # BugGuide taxon search results: links to /node/view/<id>
    # The first result is usually the best match
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Match /node/view/NNNNNN or node/view/NNNNNN
        m = re.search(r"node/view/(\d+)", href)
        if m:
            node_id = m.group(1)
            name = a.get_text(strip=True)
            # Skip navigation/menu links — taxon names are italic or in result divs
            # Basic heuristic: name should contain the search term (case-insensitive)
            if taxon_name.split()[0].lower() in name.lower() or len(name) > 3:
                return node_id, name

    return None, None


def bugguide_node_image_url(node_id: str, page: int = 0) -> str:
    """
    BugGuide image listing URL for a taxon node.
    page=0 is first page; BugGuide uses bgpage=N*24 offset.
    """
    offset = page * 24
    return f"{BUGGUIDE_NODE}/{node_id}/bgimage?offset={offset}"


# ══════════════════════════════════════════════════════════════════════════════
# BUGGUIDE PAGE PARSERS
# ══════════════════════════════════════════════════════════════════════════════

def parse_image_listing_page(html: str) -> tuple[list[dict], bool]:
    """
    Parse a BugGuide image listing page (taxon node /bgimage).
    Returns (list_of_stub_dicts, has_next_page).

    Each stub has: image_url, image_node_id, thumb_url, taxon_name.
    Full details are fetched per-image in parse_image_detail_page().
    """
    soup = BeautifulSoup(html, "html.parser")
    stubs = []

    # BugGuide image thumbnails are in <div class="bgimage-node"> or similar
    # Each thumbnail links to /node/view/<id>/bgimage/<id>
    # We look for links whose href matches the image node pattern
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Image detail links: /node/view/NNNNNN/bgimage/MMMMMM
        m = re.search(r"node/view/(\d+)/bgimage/(\d+)", href)
        if not m:
            # Also: /node/view/MMMMMM (direct image node)
            m2 = re.search(r"node/view/(\d+)", href)
            if m2:
                img_id = m2.group(1)
                if img_id in seen:
                    continue
                # Check if the link context looks like an image (has an img child)
                if a.find("img"):
                    seen.add(img_id)
                    thumb = a.find("img")
                    stubs.append({
                        "image_node_id": img_id,
                        "image_url": f"{BUGGUIDE_BASE}/node/view/{img_id}",
                        "thumb_url": thumb["src"] if thumb and thumb.get("src") else "",
                        "taxon_name": "",
                    })
            continue

        taxon_node = m.group(1)
        img_node   = m.group(2)
        if img_node in seen:
            continue
        seen.add(img_node)
        thumb = a.find("img")
        stubs.append({
            "image_node_id": img_node,
            "image_url": f"{BUGGUIDE_BASE}/node/view/{img_node}",
            "thumb_url": thumb["src"] if thumb and thumb.get("src") else "",
            "taxon_name": "",
        })

    # Detect next page: BugGuide has a "next" link or offset-based pagination
    has_next = bool(soup.find("a", string=re.compile(r"next", re.I)) or
                    soup.find("a", href=re.compile(r"offset=\d+")))

    return stubs, has_next


def parse_image_detail_page(html: str, image_url: str) -> dict | None:
    """
    Parse a BugGuide individual image/observation page.
    Extracts all available fields: remarks, behavior, life stage, size, sex,
    photographer, date, location, taxon, caption, and the full body text.
    Returns a flat dict of extracted fields, or None if page is unreadable.
    """
    soup = BeautifulSoup(html, "html.parser")

    record = {
        "image_url": image_url,
        "taxon_name": "",
        "photographer": "",
        "date": "",
        "location": "",
        "remarks": "",
        "behavior": "",
        "life_stage": "",
        "size": "",
        "sex": "",
        "caption": "",
        "body_text": "",
        "photo_url": "",
    }

    # ── Taxon name ─────────────────────────────────────────────────────────────
    # BugGuide pages have the taxon in <h1> or in breadcrumb / classification block
    h1 = soup.find("h1")
    if h1:
        record["taxon_name"] = h1.get_text(strip=True)

    # ── Photo URL ──────────────────────────────────────────────────────────────
    # Main image is usually in a div with class "bgimage" or an <img> with large src
    for img in soup.find_all("img", src=True):
        src = img["src"]
        if any(x in src for x in ["/raw/", "/images/", "bugguide.net"]):
            if "thumb" not in src and "icon" not in src:
                record["photo_url"] = src
                break

    # ── Structured fields table ────────────────────────────────────────────────
    # BugGuide detail pages have a table or dl with field labels
    # Common pattern: <b>Remarks:</b> text or <td class="field-label">Remarks</td>
    full_text_parts = []

    # Method 1: definition lists / labeled divs
    for label_el in soup.find_all(["b", "strong", "th", "td"]):
        label_text = label_el.get_text(strip=True).rstrip(":").lower()
        if label_text in ("remarks", "notes"):
            sibling = label_el.find_next_sibling()
            if not sibling:
                sibling = label_el.parent.find_next_sibling()
            if sibling:
                val = sibling.get_text(" ", strip=True)
                record["remarks"] = val
                full_text_parts.append(val)
        elif label_text in ("behavior", "behaviour"):
            sibling = label_el.find_next_sibling() or (label_el.parent.find_next_sibling() if label_el.parent else None)
            if sibling:
                val = sibling.get_text(" ", strip=True)
                record["behavior"] = val
                full_text_parts.append(val)
        elif label_text in ("life stage", "lifestage", "stage"):
            sibling = label_el.find_next_sibling() or (label_el.parent.find_next_sibling() if label_el.parent else None)
            if sibling:
                record["life_stage"] = sibling.get_text(" ", strip=True)
        elif label_text == "size":
            sibling = label_el.find_next_sibling() or (label_el.parent.find_next_sibling() if label_el.parent else None)
            if sibling:
                record["size"] = sibling.get_text(" ", strip=True)
        elif label_text == "sex":
            sibling = label_el.find_next_sibling() or (label_el.parent.find_next_sibling() if label_el.parent else None)
            if sibling:
                record["sex"] = sibling.get_text(" ", strip=True)

    # ── Photographer / collector ───────────────────────────────────────────────
    # Usually "by <name>" or in a byline div
    byline = soup.find(class_=re.compile(r"byline|submitted|author|credit", re.I))
    if byline:
        record["photographer"] = byline.get_text(" ", strip=True)
    else:
        # Fallback: look for "by " pattern in page text
        m = re.search(r"(?:by|photo by|contributed by)\s+([\w\s.'-]{3,40})", html, re.I)
        if m:
            record["photographer"] = m.group(1).strip()

    # ── Date ──────────────────────────────────────────────────────────────────
    date_el = soup.find(class_=re.compile(r"date|created|submitted", re.I))
    if date_el:
        record["date"] = date_el.get_text(" ", strip=True)
    else:
        m = re.search(r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},?\s+\d{4}", html, re.I)
        if m:
            record["date"] = m.group(0)

    # ── Location ───────────────────────────────────────────────────────────────
    loc_el = soup.find(class_=re.compile(r"location|locality|place", re.I))
    if loc_el:
        record["location"] = loc_el.get_text(" ", strip=True)

    # ── Caption / title ────────────────────────────────────────────────────────
    # Often the page <title> minus "BugGuide.Net: " prefix
    title_tag = soup.find("title")
    if title_tag:
        cap = title_tag.get_text(strip=True)
        cap = re.sub(r"BugGuide\.Net\s*[:\-–]\s*", "", cap, flags=re.I)
        record["caption"] = cap

    # ── Full body text for keyword matching ────────────────────────────────────
    # Grab all visible text from the main content area
    main = (soup.find(id=re.compile(r"content|main|body", re.I)) or
            soup.find(class_=re.compile(r"content|main|node", re.I)) or
            soup.body)
    if main:
        body = main.get_text(" ", strip=True)
        record["body_text"] = body[:3000]   # cap at 3000 chars
        full_text_parts.append(body)

    record["_combined"] = " ".join(full_text_parts).lower()

    return record


# ══════════════════════════════════════════════════════════════════════════════
# KEYWORD FILTERING
# ══════════════════════════════════════════════════════════════════════════════

def combined_text(record: dict) -> str:
    """Return the combined lowercased searchable text for a record."""
    return record.get("_combined") or (
        " ".join([
            record.get("remarks", ""),
            record.get("behavior", ""),
            record.get("life_stage", ""),
            record.get("caption", ""),
            record.get("body_text", ""),
        ]).lower()
    )


def text_has_interaction(text: str, terms: list[str]) -> bool:
    return any(t in text for t in terms)


def text_mentions_target(text: str, target_keywords: list[str]) -> bool:
    for kw in target_keywords:
        idx = text.find(kw)
        if idx == -1:
            continue
        after = text[idx + len(kw):]
        if kw in ("long-horned bee", "longhorned bee") and after.startswith("tle"):
            continue
        return True
    return False


def make_record(raw: dict, stream: str, keyword: str,
                node_id: str, job_id: str) -> dict:
    """Convert a parsed page dict into a storable result record."""
    return {
        "id": f"{node_id}_{raw['image_node_id']}_{job_id[:4]}",
        "image_node_id": raw.get("image_node_id", ""),
        "stream": stream,
        "keyword": keyword,
        "taxon_name": raw.get("taxon_name", ""),
        "photographer": raw.get("photographer", ""),
        "date": raw.get("date", ""),
        "location": raw.get("location", ""),
        "remarks": raw.get("remarks", ""),
        "behavior": raw.get("behavior", ""),
        "life_stage": raw.get("life_stage", ""),
        "size": raw.get("size", ""),
        "sex": raw.get("sex", ""),
        "caption": raw.get("caption", ""),
        "body_text": raw.get("body_text", "")[:500],
        "photo_url": raw.get("photo_url", ""),
        "url": raw.get("image_url", ""),
        "review": "",
    }


# ══════════════════════════════════════════════════════════════════════════════
# IMAGE PAGE FETCHER  (listing → detail, with dedup)
# ══════════════════════════════════════════════════════════════════════════════

def scrape_node_images(node_id: str, job: dict, log_fn,
                       check_fn,   # (combined_text) -> bool — True = keep record
                       stream_label: str,
                       keyword: str,
                       max_pages: int = 200) -> list[dict]:
    """
    Paginate through all image listing pages for a BugGuide node,
    fetch each image detail page, apply check_fn, return kept records.
    """
    kept = []
    for page_num in range(max_pages):
        if job.get("cancel"):
            break

        listing_url = bugguide_node_image_url(node_id, page_num)
        html = fetch_html(listing_url, log_fn)
        if not html:
            log_fn(f"      page {page_num+1}: could not fetch listing — stopping", "warn")
            break

        stubs, has_next = parse_image_listing_page(html)
        if not stubs:
            break

        log_fn(f"      listing page {page_num+1}: {len(stubs)} image(s)")

        for stub in stubs:
            if job.get("cancel"):
                break
            img_id = stub["image_node_id"]

            with job["_seen_lock"]:
                if img_id in job["_seen_ids"]:
                    continue
                job["_seen_ids"].add(img_id)

            # Fetch detail page
            detail_html = fetch_html(stub["image_url"], log_fn)
            if not detail_html:
                continue

            raw = parse_image_detail_page(detail_html, stub["image_url"])
            if not raw:
                continue
            raw["image_node_id"] = img_id
            raw["thumb_url"] = stub.get("thumb_url", "")

            ct = combined_text(raw)
            if check_fn(ct):
                record = make_record(raw, stream_label, keyword, node_id, job.get("job_id", ""))
                kept.append(record)
                job["rows"].append(record)
                log_fn(f"      ✓ kept: {raw.get('taxon_name','?')} — {stub['image_url']}", "checkpoint")

            time.sleep(0.8)   # polite delay between image detail requests

        if not has_next:
            break
        time.sleep(1.5)   # polite delay between listing pages

    return kept


# ══════════════════════════════════════════════════════════════════════════════
# STREAM 1 — Direct target taxon scan
# ══════════════════════════════════════════════════════════════════════════════

def stream1_worker(job_id: str):
    """
    Paginate the target taxon's BugGuide node image pages.
    Keep records whose combined text contains ANY interaction keyword.
    (We're already scoped to the target taxon — so any interaction mention is relevant.)
    """
    job = jobs[job_id]
    terms      = job["interaction_terms"]
    target_node = job["target_node_id"]
    target_name = job["target_name"]

    def log(msg, level="info"):
        job["log_queue"].put({"msg": msg, "level": level, "ts": datetime.utcnow().isoformat()})

    log(f"▶ Stream 1 — Direct {target_name} scan on BugGuide (node {target_node})", "milestone")

    if not target_node:
        log(f"✗ No BugGuide node found for '{target_name}' — Stream 1 skipped.", "error")
    else:
        def check(ct):
            return text_has_interaction(ct, terms)

        records = scrape_node_images(
            target_node, job, log,
            check_fn=check,
            stream_label="Stream 1 — Direct scan",
            keyword="[interaction keyword]",
        )
        log(f"✓ Stream 1 complete: {len(records)} records kept.", "milestone")

    job["stream1_done"] = True
    _maybe_finish(job_id)


# ══════════════════════════════════════════════════════════════════════════════
# STREAM 2 — Predator list mode
# ══════════════════════════════════════════════════════════════════════════════

def stream2_worker(job_id: str):
    """
    For each taxon in the predator list:
      1. Resolve BugGuide node ID
      2. Paginate its image records
      3. Keep records whose text mentions the target taxon
    """
    job = jobs[job_id]
    raw_list       = job.get("predator_list", "")
    target_keywords = job["target_keywords"]
    resume_idx     = job.get("predator_index_done", 0)

    def log(msg, level="info"):
        job["log_queue"].put({"msg": msg, "level": level, "ts": datetime.utcnow().isoformat()})

    names = [n.strip() for n in raw_list.replace(",", "\n").replace(";", "\n").splitlines() if n.strip()]
    total = len(names)
    log(f"▶ Stream 2 — Predator list scan: {total} taxa ({resume_idx} already done)", "milestone")

    if resume_idx > 0:
        names = names[resume_idx:]

    for i, name in enumerate(names, resume_idx + 1):
        if job.get("cancel"):
            log("⛔ Cancelled.", "warn")
            break

        job["predator_index_done"] = i
        log(f"[{i}/{total}] Resolving '{name}' on BugGuide…")

        node_id, canonical = resolve_bugguide_node(name, log)
        if not node_id:
            log(f"  '{name}' — not found on BugGuide; skipping", "warn")
            time.sleep(1)
            continue

        log(f"  '{name}' → '{canonical}' (node {node_id})")

        def check(ct, tk=target_keywords):
            return text_mentions_target(ct, tk)

        records = scrape_node_images(
            node_id, job, log,
            check_fn=check,
            stream_label="Stream 2 — Predator list",
            keyword=name,
        )
        log(f"  ✓ '{name}': {len(records)} record(s) kept (total: {len(job['rows'])})",
            "checkpoint" if records else "info")
        time.sleep(2)

    log(f"✓ Stream 2 complete. {len(job['rows'])} total records.", "milestone")
    job["stream2_done"] = True
    _maybe_finish(job_id)


# ══════════════════════════════════════════════════════════════════════════════
# STREAM 3 — All Records (sitewide BugGuide search)
# ══════════════════════════════════════════════════════════════════════════════

def _bugguide_search_page(query: str, page_offset: int, log_fn) -> tuple[list[dict], bool]:
    """
    Fetch one page of BugGuide sitewide image search results for `query`.
    Returns (stubs, has_next).
    BugGuide search: /index.php?q=search&ts=img&search=<query>&offset=N
    """
    url = (
        f"{BUGGUIDE_SEARCH}?q=search&ts=img"
        f"&search={requests.utils.quote(query)}"
        f"&offset={page_offset}"
    )
    html = fetch_html(url, log_fn)
    if not html:
        return [], False

    stubs, has_next = parse_image_listing_page(html)

    # Also check if BugGuide returned a "no results" message
    if "no records" in html.lower() or "0 records" in html.lower():
        return [], False

    return stubs, has_next


def stream3_worker(job_id: str):
    """
    All Records mode: search BugGuide sitewide for each target keyword.
    Each search returns image records where that keyword appears anywhere
    in the BugGuide record (taxon name, remarks, caption, etc.).
    Keep records that ALSO contain at least one interaction keyword.
    """
    job = jobs[job_id]
    target_keywords  = job["target_keywords"]
    interaction_terms = job["interaction_terms"]
    resume_kw_idx    = job.get("stream3_kw_index", 0)

    def log(msg, level="info"):
        job["log_queue"].put({"msg": msg, "level": level, "ts": datetime.utcnow().isoformat()})

    log(f"▶ Stream 3 — All Records sitewide search ({len(target_keywords)} target keyword(s))", "milestone")
    log("  Searching BugGuide image index for all records mentioning the target taxon.", "info")

    total_kept = 0

    for kw_i, kw in enumerate(target_keywords):
        if kw_i < resume_kw_idx:
            continue
        if job.get("cancel"):
            log("⛔ Cancelled.", "warn")
            break

        job["stream3_kw_index"] = kw_i
        log(f"[{kw_i+1}/{len(target_keywords)}] Sitewide search: '{kw}'")

        page_offset = 0
        page_num    = 0
        kw_kept     = 0

        while True:
            if job.get("cancel"):
                break

            stubs, has_next = _bugguide_search_page(kw, page_offset, log)
            if not stubs:
                if page_num == 0:
                    log(f"  '{kw}': no results found")
                break

            log(f"  '{kw}' page {page_num+1}: {len(stubs)} result(s)")

            for stub in stubs:
                if job.get("cancel"):
                    break
                img_id = stub["image_node_id"]

                with job["_seen_lock"]:
                    if img_id in job["_seen_ids"]:
                        continue
                    job["_seen_ids"].add(img_id)

                detail_html = fetch_html(stub["image_url"], log)
                if not detail_html:
                    continue

                raw = parse_image_detail_page(detail_html, stub["image_url"])
                if not raw:
                    continue
                raw["image_node_id"] = img_id
                raw["thumb_url"] = stub.get("thumb_url", "")

                ct = combined_text(raw)

                # Must mention target AND have an interaction signal
                if text_mentions_target(ct, [kw]) and text_has_interaction(ct, interaction_terms):
                    record = make_record(raw, "Stream 3 — All Records", kw, "sitewide", job_id)
                    job["rows"].append(record)
                    kw_kept += 1
                    total_kept += 1
                    log(f"    ✓ kept: {raw.get('taxon_name','?')} — {stub['image_url']}", "checkpoint")

                time.sleep(0.8)

            if not has_next:
                break

            page_offset += 24
            page_num    += 1
            time.sleep(1.5)

        log(f"  ✓ '{kw}': {kw_kept} record(s) kept (grand total: {len(job['rows'])})",
            "checkpoint" if kw_kept else "info")

    log(f"✓ Stream 3 complete: {total_kept} new records found.", "milestone")
    job["stream3_done"] = True
    _maybe_finish(job_id)


# ══════════════════════════════════════════════════════════════════════════════
# JOB COMPLETION
# ══════════════════════════════════════════════════════════════════════════════

def _maybe_finish(job_id: str):
    """Mark job done when all expected streams have finished."""
    job = jobs[job_id]
    mode = job["mode"]

    if mode == "stream1_only":
        if job.get("stream1_done"):
            _finish(job_id)
    elif mode == "predator_list":
        if job.get("stream1_done") and job.get("stream2_done"):
            _finish(job_id)
    elif mode == "all_records":
        if job.get("stream1_done") and job.get("stream3_done"):
            _finish(job_id)
    elif mode == "all":
        if job.get("stream1_done") and job.get("stream2_done") and job.get("stream3_done"):
            _finish(job_id)


def _finish(job_id: str):
    job = jobs[job_id]
    job["status"] = "done"
    job["log_queue"].put({
        "status": "done",
        "total": len(job["rows"]),
        "msg": f"✓ All streams complete. {len(job['rows'])} total records.",
        "level": "milestone",
        "ts": datetime.utcnow().isoformat(),
    })


# ══════════════════════════════════════════════════════════════════════════════
# SEQUENTIAL RUNNER  (streams run one after another in a single thread
#                     so SSE connection stays alive throughout)
# ══════════════════════════════════════════════════════════════════════════════

def run_all_streams(job_id: str):
    job = jobs[job_id]
    mode = job["mode"]

    # Stream 1 always runs
    stream1_worker(job_id)

    if job.get("cancel"):
        return

    if mode in ("predator_list", "all") and job.get("predator_list", "").strip():
        stream2_worker(job_id)

    if job.get("cancel"):
        return

    if mode in ("all_records", "all"):
        stream3_worker(job_id)


# ══════════════════════════════════════════════════════════════════════════════
# FLASK ROUTES
# ══════════════════════════════════════════════════════════════════════════════
@app.route("/ping")
def ping():
    return jsonify({"status": "ok"}), 200

@app.route("/")
def index():
    return jsonify({"status": "ok"}), 200

@app.route("/api/taxon/search")
def taxon_search():
    """Resolve a taxon name to a BugGuide node ID."""
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify([])
    node_id, name = resolve_bugguide_node(q)
    if node_id:
        return jsonify([{"node_id": node_id, "name": name,
                         "url": f"{BUGGUIDE_NODE}/{node_id}"}])
    return jsonify([])


@app.route("/api/mine/start", methods=["POST"])
def mine_start():
    data = request.get_json(force=True)

    target_node   = str(data.get("target_node_id", "")).strip()
    target_name   = str(data.get("target_name", "Unknown")).strip()
    mode          = data.get("mode", "stream1_only")   # stream1_only | predator_list | all_records | all
    predator_list = data.get("predator_list", "")
    preloaded     = data.get("preloaded_rows", [])
    resume_pred   = int(data.get("resume_predator_index", 0))
    resume_kw     = int(data.get("resume_stream3_kw_index", 0))

    custom_interaction = data.get("custom_interaction_terms", [])
    custom_target      = data.get("custom_target_keywords", [])

    interaction_terms  = custom_interaction if custom_interaction else list(DEFAULT_INTERACTION_TERMS)
    target_keywords    = custom_target      if custom_target      else list(DEFAULT_TARGET_KEYWORDS)

    if not target_node:
        return jsonify({"error": "target_node_id required"}), 400

    job_id = str(uuid.uuid4())[:8]

    jobs[job_id] = {
        "job_id":               job_id,
        "status":               "running",
        "mode":                 mode,
        "target_node_id":       target_node,
        "target_name":          target_name,
        "predator_list":        predator_list,
        "interaction_terms":    interaction_terms,
        "target_keywords":      target_keywords,
        "rows":                 list(preloaded),
        "log_queue":            queue.Queue(),
        "stream1_done":         False,
        "stream2_done":         False,
        "stream3_done":         False,
        "predator_index_done":  resume_pred,
        "stream3_kw_index":     resume_kw,
        "cancel":               False,
        "started_at":           datetime.utcnow().isoformat(),
        "_seen_ids":            set(r.get("image_node_id", "") for r in preloaded),
        "_seen_lock":           threading.Lock(),
    }

    thread = threading.Thread(target=run_all_streams, args=(job_id,), daemon=True)
    thread.start()

    return jsonify({"job_id": job_id, "target_name": target_name})


@app.route("/api/mine/<job_id>/stream")
def mine_stream(job_id):
    if job_id not in jobs:
        return jsonify({"error": "not found"}), 404

    def generate():
        job = jobs[job_id]
        q   = job["log_queue"]
        while True:
            try:
                item = q.get(timeout=25)
            except queue.Empty:
                yield f"data: {json.dumps({'heartbeat': True, 'total_rows': len(job['rows']), 'status': job['status'], 'predator_index_done': job.get('predator_index_done',0), 'stream3_kw_index': job.get('stream3_kw_index',0)})}\\n\\n"
                if job["status"] == "done":
                    break
                continue

            payload = {
                **item,
                "total_rows": len(job["rows"]),
                "status":     job["status"],
            }
            yield f"data: {json.dumps(payload)}\\n\\n"
            if item.get("status") == "done":
                break

    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/api/mine/<job_id>/rows")
def mine_rows(job_id):
    if job_id not in jobs:
        return jsonify({"error": "not found"}), 404
    return jsonify(jobs[job_id]["rows"])


@app.route("/api/mine/<job_id>/status")
def mine_status(job_id):
    if job_id not in jobs:
        return jsonify({"error": "not found"}), 404
    job = jobs[job_id]
    return jsonify({
        "status":               job["status"],
        "total_rows":           len(job["rows"]),
        "predator_index_done":  job.get("predator_index_done", 0),
        "stream3_kw_index":     job.get("stream3_kw_index", 0),
    })


@app.route("/api/mine/<job_id>/cancel", methods=["POST"])
def mine_cancel(job_id):
    if job_id not in jobs:
        return jsonify({"error": "not found"}), 404
    jobs[job_id]["cancel"] = True
    return jsonify({"ok": True})


@app.route("/api/mine/<job_id>/checkpoint")
def mine_checkpoint(job_id):
    if job_id not in jobs:
        return jsonify({"error": "not found"}), 404
    job = jobs[job_id]
    return jsonify({
        "job_id":               job_id,
        "target_node_id":       job["target_node_id"],
        "target_name":          job["target_name"],
        "mode":                 job["mode"],
        "predator_list":        job.get("predator_list", ""),
        "status":               job["status"],
        "total_rows":           len(job["rows"]),
        "rows":                 job["rows"],
        "predator_index_done":  job.get("predator_index_done", 0),
        "stream3_kw_index":     job.get("stream3_kw_index", 0),
        "stream1_done":         job.get("stream1_done", False),
        "stream2_done":         job.get("stream2_done", False),
        "stream3_done":         job.get("stream3_done", False),
        "saved_at":             datetime.utcnow().isoformat(),
    })


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7860, debug=False)