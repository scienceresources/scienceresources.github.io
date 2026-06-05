"""
Literature Miner — Flask Backend
Dynamic taxon search, BHL + OpenAlex/Crossref/S2/PubMed integration,
robust .litminer checkpoint system, SSE live log streaming.
"""

from flask import Flask, request, jsonify, send_from_directory, Response, stream_with_context
from flask_cors import CORS
import requests as req_lib
import pandas as pd
import threading
import time
import re
import os
import json
import uuid
import queue
import io
import zipfile
import base64
import xml.etree.ElementTree as ET
import warnings

warnings.filterwarnings("ignore")

app = Flask(__name__, static_folder="static")
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024
CORS(app)

# ══════════════════════════════════════════════════════════════════════════════
# IN-MEMORY JOB STORE + GLOBAL QUEUE (single-worker serialization)
# ══════════════════════════════════════════════════════════════════════════════
JOBS: dict[str, dict] = {}
JOBS_LOCK   = threading.Lock()
JOB_QUEUE: queue.Queue = queue.Queue()   # FIFO queue of (job_id, cfg, taxa_text, resume_state)
ACTIVE_JOB: dict = {"job_id": None}     # which job is currently running
QUEUE_LOCK  = threading.Lock()

def _queue_worker():
    """Single background thread that processes one job at a time."""
    while True:
        item = JOB_QUEUE.get()          # blocks until a job is available
        if item is None:
            break                        # sentinel — shutdown
        job_id, cfg, taxa_text, resume_state = item
        with QUEUE_LOCK:
            ACTIVE_JOB["job_id"] = job_id
        if job_id in JOBS:
            JOBS[job_id]["queue_status"] = "running"
            job_log(job_id, "▶ Job dequeued — starting now.", "info")
        try:
            run_job(job_id, cfg, taxa_text, resume_state)
        except Exception:
            pass
        with QUEUE_LOCK:
            ACTIVE_JOB["job_id"] = None
        JOB_QUEUE.task_done()

_QUEUE_THREAD = threading.Thread(target=_queue_worker, daemon=True)
_QUEUE_THREAD.start()


def new_job() -> str:
    job_id = str(uuid.uuid4())
    JOBS[job_id] = {
        "log":            queue.Queue(),
        "status":         "queued",          # queued → running → done/error/stopped
        "queue_status":   "queued",
        "rows":           [],
        "csv":            "",
        "error_msg":      "",
        "stop_flag":      False,
        # Checkpoint state (serializable)
        "checkpoint": {
            "seen_keys":        {},
            "seen_bhl_ids":     {},
            "direct_hits":      [],
            "fulltext_queue":   [],
            "bhl_ocr_queue":    [],
            "bhl_direct_hits":  [],
            "all_rows":         [],
            "std_done":         [],
            "pred_done":        [],
            "bhl_std_done":     [],
            "bhl_pred_done":    [],
            "ocr_hits":         [],
            "fetched_ocr":      {},
            "phase":            "search",
            "taxon_info":       {},
            "query_terms":      {},
            "progress": {
                "std_total": 0, "std_done": 0,
                "pred_total": 0, "pred_done": 0,
                "bhl_std_total": 0, "bhl_std_done": 0,
                "bhl_pred_total": 0, "bhl_pred_done": 0,
                "ft_total": 0, "ft_done": 0,
                "ocr_total": 0, "ocr_done": 0,
            },
            "stats": {
                "total_hits": 0, "check_hits": 0, "noise_hits": 0,
                "api_calls": 0, "req_log": [],
            },
        },
    }
    return job_id


def job_log(job_id: str, msg: str, level: str = "info"):
    if job_id in JOBS:
        JOBS[job_id]["log"].put({"msg": msg, "level": level, "ts": time.time()})


# ══════════════════════════════════════════════════════════════════════════════
# SEARCH SETTINGS
# ══════════════════════════════════════════════════════════════════════════════
RESULTS_PER_QUERY = 25
FETCH_FULLTEXT    = True
BHL_API_KEY       = ""          # Set at runtime via UI — never hardcoded
BHL_BASE          = "https://www.biodiversitylibrary.org/api3"
BHL_RATE_LIMIT    = 0.25
MAX_PAGES_PER_QUERY = 10
MAX_OCR_PAGES     = 12
CHECKPOINT_INTERVAL_STD  = 50
CHECKPOINT_INTERVAL_PRED = 100
CHECKPOINT_INTERVAL_OCR  = 25

RATE = {
    "openalex":         0.35,
    "crossref":         0.15,
    "semantic_scholar": 1.10,
    "pubmed":           0.40,
}

OPENALEX_BASE  = "https://api.openalex.org"
CROSSREF_BASE  = "https://api.crossref.org"
S2_BASE        = "https://api.semanticscholar.org/graph/v1"
PUBMED_ESEARCH = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
PUBMED_EFETCH  = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
GBIF_BASE      = "https://api.gbif.org/v1"
INAT_BASE      = "https://api.inaturalist.org/v1"

OA_DOMAINS = [
    "ncbi.nlm.nih.gov/pmc", "europepmc.org", "arxiv.org",
    "biorxiv.org", "medrxiv.org", "zenodo.org", "peerj.com",
    "frontiersin.org", "mdpi.com", "plos", "elifesciences.org",
    "royalsocietypublishing.org", "jstage.jst.go.jp",
    "pensoft.net", "biodiversity-science.net",
]

TIGHT_RADIUS        = 300
TRIGGER_PADDING     = 600
MEL_CONTEXT_PADDING = 400

# ══════════════════════════════════════════════════════════════════════════════
# BEE/TARGET SIGNAL / NOISE FILTERS
# ══════════════════════════════════════════════════════════════════════════════

BEE_SIGNAL_TERMS = {
    "melissodes","bee","bees","apidae","anthophoridae","eucerini","apoidea",
    "apinae","halictidae","colletidae","andrenidae","megachilidae",
    "bombus","apis","halictus","lasioglossum","andrena","colletes",
    "xylocopa","ceratina","agapostemon","peponapis","xenoglossa",
    "svastra","tetraloniella","florilegus","martinapis","long-horned",
    "solitary bee","ground bee","native bee","pollinator",
}

HARD_BLOCKLIST_TITLE = [
    "flowers and insects", "lists of visitors", "bibliographia zoologica",
    "cresson types of hymenoptera", "index to information on insects",
    "official lists and indexes",
]

SOFT_BLOCKLIST_TITLE = [
    "catalog of hymenoptera", "synoptic catalog",
    "catalogue of hymenopterous insects",
]

_ECO_RE = re.compile(
    r"(parasit|predator|\bprey\b|host:|kleptoparasit|cleptoparasit|"
    r"provisions|stylopiz|reared from|emerged from|taken from|collected from|"
    r"natural enem|captured by|attacked by)", re.I,
)

ENEMY_KEYWORDS = [
    "parasit","kleptoparasit","cleptoparasit","predator","prey",
    "natural enemy","natural enemies","host","parasitoid","parasitize",
    "stylopiz","strepsipter","reared from","emerged from",
    "taken from nest","collected from nest","nest provisions",
    "brood parasit","nest parasit","captured by","attacked by",
    "consumed by","preyed upon",
]

_MEL_RE = re.compile(r"(?i)melissodes")


def _has_bee_signal(title, abstract):
    combined = (str(title) + " " + (abstract or "")).lower()
    return any(t in combined for t in BEE_SIGNAL_TERMS)


def _has_eco_signal(text):
    return bool(_ECO_RE.search(str(text)))


def _title_is_hard_blocked(title):
    t = str(title).lower()
    return any(frag in t for frag in HARD_BLOCKLIST_TITLE)


def _title_is_soft_blocked(title):
    t = str(title).lower()
    return any(frag in t for frag in SOFT_BLOCKLIST_TITLE)


def _find_nearest_keyword(text_lower, mel_start, radius):
    best = None
    best_dist = radius + 1
    s = max(0, mel_start - radius)
    e = min(len(text_lower), mel_start + radius)
    window = text_lower[s:e]
    for kw in ENEMY_KEYWORDS:
        idx = window.find(kw)
        if idx >= 0:
            abs_idx = s + idx
            dist = abs(abs_idx - mel_start)
            if dist < best_dist:
                best_dist = dist
                best = (kw, abs_idx, abs_idx + len(kw))
    return best


def _noise_reason(trigger, title="", target_term="melissodes"):
    t   = str(trigger)
    tl  = t.lower()
    eco = _has_eco_signal(t)
    if _title_is_hard_blocked(title):
        return "BLOCKED_TITLE"
    if _title_is_soft_blocked(title) and not eco:
        return "BLOCKED_TITLE_NO_ECO"
    if re.search(r"Long-tongued Bees|Short-tongued Bees", t):
        return "FLOWER_VISITOR_LIST"
    if re.search(r"P '?\d{2},\s*\d+", t):
        return "JOURNAL_INDEX"
    if len(re.findall(r"[A-Z][a-z]+[\s_\-\.]{3,}\d{3,4}", t)) >= 3:
        return "BOOK_INDEX"
    if len(re.findall(r"\b(1[89]\d{2}|20\d{2})\.\s+[A-Z]", t)) >= 4 and not eco:
        return "BIBLIOGRAPHY"
    if re.search(r"literature cited|references cited", tl) and not eco:
        return "BIBLIOGRAPHY"
    return ""


def _build_trigger(text, mel_s, mel_e, kw_s, kw_e, pad=TRIGGER_PADDING):
    s = max(0, min(mel_s, kw_s) - pad)
    e = min(len(text), max(mel_e, kw_e) + pad)
    frag = re.sub(r"\s+", " ", text[s:e]).strip()
    return ("…" if s > 0 else "") + frag + ("…" if e < len(text) else "")


def _mel_context(text, mel_s, mel_e, pad=MEL_CONTEXT_PADDING):
    s = max(0, mel_s - pad)
    e = min(len(text), mel_e + pad)
    frag = re.sub(r"\s+", " ", text[s:e]).strip()
    return ("…" if s > 0 else "") + frag + ("…" if e < len(text) else "")


def _target_re(target_term):
    return re.compile(re.escape(target_term), re.I)


def extract_hits_standard(text, title="", target_term="melissodes"):
    pat = _target_re(target_term)
    if not text or target_term.lower() not in text.lower():
        return []
    lower = text.lower()
    results = []
    seen_spans = []
    for m in pat.finditer(text):
        mel_s, mel_e = m.start(), m.end()
        if any(s <= mel_s <= e for s, e in seen_spans):
            continue
        hit = _find_nearest_keyword(lower, mel_s, TIGHT_RADIUS)
        if not hit:
            continue
        kw, kw_s, kw_e = hit
        trigger = _build_trigger(text, mel_s, mel_e, kw_s, kw_e)
        reason  = _noise_reason(trigger, title, target_term)
        verdict = "NOISE" if reason else "CHECK"
        results.append((trigger, kw, verdict, reason))
        seen_spans.append((mel_s - TRIGGER_PADDING, mel_e + TRIGGER_PADDING))
    return results


def extract_hits_predator(text, title="", max_snippets=5, target_term="melissodes"):
    pat = _target_re(target_term)
    if not text or target_term.lower() not in text.lower():
        return []
    snippets = []
    seen_pos = []
    for m in pat.finditer(text):
        mel_s, mel_e = m.start(), m.end()
        if any(abs(mel_s - p) < MEL_CONTEXT_PADDING for p in seen_pos):
            continue
        snippets.append(_mel_context(text, mel_s, mel_e))
        seen_pos.append(mel_s)
        if len(snippets) >= max_snippets:
            break
    if not snippets:
        return []
    lower     = text.lower()
    mel_count = len(pat.findall(text))
    combined  = (
        f"[{mel_count} occurrences total — showing up to {len(snippets)}]\n\n"
        + "\n\n——\n\n".join(snippets)
    )
    first_mel = lower.index(target_term.lower())
    hit = _find_nearest_keyword(lower, first_mel, len(text))
    best_kw = hit[0] if hit else target_term
    reason  = _noise_reason(combined, title, target_term)
    verdict = "NOISE" if reason else "CHECK"
    return [(combined, best_kw, verdict, reason, mel_count)]


def check_abstract_snippet(text, title="", target_term="melissodes"):
    pat = _target_re(target_term)
    lower = str(text).lower()
    if target_term.lower() not in lower:
        return None
    for m in pat.finditer(lower):
        hit = _find_nearest_keyword(lower, m.start(), TIGHT_RADIUS)
        if hit:
            kw, kw_s, kw_e = hit
            trigger = _build_trigger(text, m.start(), m.start() + 10, kw_s, kw_e)
            reason  = _noise_reason(trigger, title, target_term)
            return trigger, kw, ("NOISE" if reason else "CHECK"), reason
    return None


# ══════════════════════════════════════════════════════════════════════════════
# DEDUP / UTILITY
# ══════════════════════════════════════════════════════════════════════════════

def _dedup_key(doi, title, year):
    if doi and doi.strip():
        return f"doi:{doi.lower().strip()}"
    t = re.sub(r'\W+', '', str(title).lower())[:80]
    return f"title:{t}:{year}"


def _clean_html(text):
    if not text:
        return ""
    text = re.sub(r'<[^>]+>', ' ', text)
    return re.sub(r'\s+', ' ', text).strip()


def _reconstruct_abstract(inv_idx):
    if not inv_idx:
        return ""
    pos_word = {}
    for word, positions in inv_idx.items():
        for p in positions:
            pos_word[p] = word
    return " ".join(pos_word[k] for k in sorted(pos_word))


# ══════════════════════════════════════════════════════════════════════════════
# API SEARCH FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def _log_req(job_id, source, query, status):
    if job_id and job_id in JOBS:
        cp = JOBS[job_id]["checkpoint"]
        cp["stats"]["api_calls"] += 1
        entry = {"source": source, "query": query[:60], "status": status, "ts": time.time()}
        cp["stats"]["req_log"].append(entry)
        if len(cp["stats"]["req_log"]) > 500:
            cp["stats"]["req_log"] = cp["stats"]["req_log"][-500:]


def search_openalex(query, email, job_id=None):
    results = []
    params = {
        "search": query,
        "per-page": RESULTS_PER_QUERY,
        "mailto": email,
        "select": "id,doi,title,authorships,publication_year,open_access,abstract_inverted_index",
    }
    try:
        r = req_lib.get(f"{OPENALEX_BASE}/works", params=params,
                        headers={"Accept": "application/json",
                                 "User-Agent": f"mailto:{email}"},
                        timeout=20)
        _log_req(job_id, "openalex", query, r.status_code)
        r.raise_for_status()
        for item in r.json().get("results", []):
            inv = item.get("abstract_inverted_index") or {}
            abstract = _reconstruct_abstract(inv) if isinstance(inv, dict) else ""
            authors = item.get("authorships", [])
            author = authors[0].get("author", {}).get("display_name", "") if authors else ""
            oa_url = (item.get("open_access") or {}).get("oa_url") or ""
            doi = (item.get("doi") or "").replace("https://doi.org/", "")
            results.append({
                "title": _clean_html(item.get("title", "")),
                "author": author,
                "year": str(item.get("publication_year", "")),
                "doi": doi,
                "url": item.get("doi") or item.get("id", ""),
                "oa_url": oa_url,
                "abstract": abstract,
                "source": "openalex",
                "source_id": item.get("id", ""),
            })
    except Exception as e:
        _log_req(job_id, "openalex", query, f"ERR:{e}")
    time.sleep(RATE["openalex"])
    return results


def search_crossref(query, email, job_id=None):
    results = []
    params = {
        "query": query,
        "rows": RESULTS_PER_QUERY,
        "mailto": email,
        "select": "DOI,title,author,published,abstract,URL,link",
    }
    try:
        r = req_lib.get(f"{CROSSREF_BASE}/works", params=params, timeout=20)
        _log_req(job_id, "crossref", query, r.status_code)
        r.raise_for_status()
        for item in r.json().get("message", {}).get("items", []):
            title = ""
            if item.get("title"):
                title = item["title"][0] if isinstance(item["title"], list) else item["title"]
            authors = item.get("author", [])
            author = ""
            if authors:
                a = authors[0]
                author = f"{a.get('given','')} {a.get('family','')}".strip()
            year = ""
            pub = item.get("published", {})
            if pub and pub.get("date-parts"):
                year = str(pub["date-parts"][0][0])
            oa_url = ""
            for link in item.get("link", []):
                if link.get("content-type") == "text/html":
                    oa_url = link.get("URL", "")
                    break
            results.append({
                "title": _clean_html(title),
                "author": author,
                "year": year,
                "doi": item.get("DOI", ""),
                "url": item.get("URL", ""),
                "oa_url": oa_url,
                "abstract": _clean_html(item.get("abstract", "")),
                "source": "crossref",
                "source_id": item.get("DOI", ""),
            })
    except Exception as e:
        _log_req(job_id, "crossref", query, f"ERR:{e}")
    time.sleep(RATE["crossref"])
    return results


def search_semantic_scholar(query, s2_key=None, job_id=None):
    results = []
    headers = {"x-api-key": s2_key} if s2_key else {}
    params = {
        "query": query,
        "limit": RESULTS_PER_QUERY,
        "fields": "title,authors,year,externalIds,abstract,openAccessPdf,url",
    }
    try:
        r = req_lib.get(f"{S2_BASE}/paper/search", params=params, headers=headers, timeout=20)
        _log_req(job_id, "s2", query, r.status_code)
        r.raise_for_status()
        for item in r.json().get("data", []):
            ext = item.get("externalIds", {}) or {}
            doi = ext.get("DOI", "")
            oa_url = (item.get("openAccessPdf") or {}).get("url", "")
            authors = item.get("authors", [])
            author = authors[0].get("name", "") if authors else ""
            results.append({
                "title": item.get("title", ""),
                "author": author,
                "year": str(item.get("year", "")),
                "doi": doi,
                "url": item.get("url", "") or (f"https://doi.org/{doi}" if doi else ""),
                "oa_url": oa_url,
                "abstract": item.get("abstract", "") or "",
                "source": "semantic_scholar",
                "source_id": item.get("paperId", ""),
            })
    except Exception as e:
        _log_req(job_id, "s2", query, f"ERR:{e}")
    time.sleep(RATE["semantic_scholar"])
    return results


def search_pubmed(query, pubmed_key=None, job_id=None):
    results = []
    params = {
        "db": "pubmed", "term": query,
        "retmax": RESULTS_PER_QUERY, "retmode": "json",
    }
    if pubmed_key:
        params["api_key"] = pubmed_key
    try:
        r = req_lib.get(PUBMED_ESEARCH, params=params, timeout=20)
        _log_req(job_id, "pubmed", query, r.status_code)
        r.raise_for_status()
        ids = r.json().get("esearchresult", {}).get("idlist", [])
        if not ids:
            time.sleep(RATE["pubmed"])
            return results
        fetch_params = {
            "db": "pubmed", "id": ",".join(ids),
            "rettype": "xml", "retmode": "xml",
        }
        if pubmed_key:
            fetch_params["api_key"] = pubmed_key
        rf = req_lib.get(PUBMED_EFETCH, params=fetch_params, timeout=30)
        _log_req(job_id, "pubmed_fetch", query, rf.status_code)
        root = ET.fromstring(rf.content)
        for art in root.findall(".//PubmedArticle"):
            title = ""
            t_el = art.find(".//ArticleTitle")
            if t_el is not None:
                title = ET.tostring(t_el, encoding="unicode", method="text")
            abstract = ""
            for ab in art.findall(".//AbstractText"):
                abstract += (ET.tostring(ab, encoding="unicode", method="text") or "") + " "
            abstract = abstract.strip()
            pmid_el = art.find(".//PMID")
            pmid = pmid_el.text if pmid_el is not None else ""
            year = ""
            y_el = art.find(".//PubDate/Year")
            if y_el is None:
                y_el = art.find(".//PubDate/MedlineDate")
            if y_el is not None:
                year = (y_el.text or "")[:4]
            doi = ""
            for aid in art.findall(".//ArticleId"):
                if aid.get("IdType") == "doi":
                    doi = aid.text or ""
            author = ""
            auth_el = art.find(".//AuthorList/Author")
            if auth_el is not None:
                ln = auth_el.find("LastName")
                fn = auth_el.find("ForeName")
                author = " ".join(filter(None, [
                    fn.text if fn is not None else "",
                    ln.text if ln is not None else "",
                ]))
            results.append({
                "title": title.strip(),
                "author": author,
                "year": year,
                "doi": doi,
                "url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" if pmid else "",
                "oa_url": f"https://www.ncbi.nlm.nih.gov/pmc/articles/pmid/{pmid}" if pmid else "",
                "abstract": abstract,
                "source": "pubmed",
                "source_id": f"pmid:{pmid}",
            })
    except Exception as e:
        _log_req(job_id, "pubmed", query, f"ERR:{e}")
    time.sleep(RATE["pubmed"])
    return results


def search_all_sources(query, cfg, job_id=None):
    results = []
    if cfg["sources"].get("openalex"):
        results += search_openalex(query, cfg["email"], job_id)
    if cfg["sources"].get("crossref"):
        results += search_crossref(query, cfg["email"], job_id)
    if cfg["sources"].get("semantic_scholar"):
        results += search_semantic_scholar(query, cfg.get("s2_key"), job_id)
    if cfg["sources"].get("pubmed"):
        results += search_pubmed(query, cfg.get("pubmed_key"), job_id)
    return results


# ══════════════════════════════════════════════════════════════════════════════
# FULL-TEXT FETCHER
# ══════════════════════════════════════════════════════════════════════════════

def fetch_fulltext(oa_url, url=""):
    for target in [u for u in [oa_url, url] if u]:
        if not any(d in target for d in OA_DOMAINS):
            continue
        try:
            r = req_lib.get(target, timeout=30,
                            headers={"User-Agent": "Mozilla/5.0 (academic research)"})
            if r.status_code == 200 and r.text:
                text = _clean_html(r.text)
                if len(text) > 200:
                    return text[:100000]
        except Exception:
            pass
    return ""


# ══════════════════════════════════════════════════════════════════════════════
# BHL API HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def bhl_get(params, job_id=None, api_key=None):
    key = api_key or BHL_API_KEY
    params = {**params, "apikey": key, "format": "json"}
    try:
        r = req_lib.get(BHL_BASE, params=params, timeout=40)
        _log_req(job_id, "bhl", params.get("op", ""), r.status_code)
        r.raise_for_status()
        raw = r.text.strip()
        if not raw or raw.startswith("<"):
            return None
        return r.json()
    except Exception as e:
        _log_req(job_id, "bhl", params.get("op", ""), f"ERR:{e}")
        return None


def bhl_run_query(search_term, query_type, job_id=None, bhl_key=None):
    results = []
    for page in range(1, MAX_PAGES_PER_QUERY + 1):
        data = bhl_get({
            "op": "PublicationSearch",
            "searchterm": search_term,
            "searchtype": "F",
            "page": page,
        }, job_id, api_key=bhl_key)
        time.sleep(BHL_RATE_LIMIT)
        if not data or data.get("Status") != "ok":
            break
        items = data.get("Result", [])
        if not items:
            break
        for item in items:
            url = (item.get("BHLPageUrl") or item.get("PartUrl") or
                   item.get("ItemUrl") or "").strip()
            if not url:
                continue
            ids = {}
            for pattern, key in [(r"/page/(\d+)", "page_id"),
                                  (r"/part/(\d+)", "part_id"),
                                  (r"/item/(\d+)", "item_id")]:
                m = re.search(pattern, url)
                if m:
                    ids[key] = m.group(1)
                    break
            results.append({
                "title":     item.get("Title","") or item.get("PublicationTitle",""),
                "author":    item.get("AuthorName","") or item.get("Author",""),
                "year":      item.get("Date","") or item.get("Year",""),
                "url":       url,
                "snippet":   item.get("TextSnippet","") or "",
                "query":     search_term,
                "query_type": query_type,
                **ids,
            })
        if len(items) < 10:
            break
    return results


def bhl_get_part_item_id(part_id, job_id=None, bhl_key=None):
    data = bhl_get({"op": "GetPartMetadata", "id": part_id,
                    "pages": "false", "names": "false"}, job_id, api_key=bhl_key)
    if not data or data.get("Status") != "ok":
        return None
    results = data.get("Result", [])
    if not results:
        return None
    return str(results[0].get("ItemID", "") or "") or None


def bhl_get_ia_identifier(item_id, job_id=None, bhl_key=None):
    data = bhl_get({"op": "GetItemMetadata", "id": item_id,
                    "pages": "false", "ocr": "false"}, job_id, api_key=bhl_key)
    if not data or data.get("Status") != "ok":
        return None
    results = data.get("Result", [])
    if not results:
        return None
    item = results[0]
    src = item.get("SourceIdentifier", "") or ""
    if src:
        return src.strip()
    for field in ("ExternalUrl", "Source"):
        val = item.get(field, "") or ""
        if "archive.org" in val:
            m = re.search(r"archive\.org/(?:details|download)/([^/?&#]+)", val)
            if m:
                return m.group(1)
    return None


def bhl_get_ocr_via_ia(item_id, job_id=None, bhl_key=None):
    ia_id = bhl_get_ia_identifier(item_id, job_id, bhl_key=bhl_key)
    if not ia_id:
        return ""
    url = f"https://archive.org/download/{ia_id}/{ia_id}_djvu.txt"
    try:
        r = req_lib.get(url, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
        _log_req(job_id, "ia_ocr", ia_id, r.status_code)
        if r.status_code != 200 or r.text.strip().startswith("<"):
            return ""
        text = r.text.strip()
        return text if len(text) > 50 else ""
    except Exception:
        return ""


def bhl_get_page_ocr(page_id, job_id=None, bhl_key=None):
    data = bhl_get({"op": "GetPageOcrText", "pageid": page_id}, job_id, api_key=bhl_key)
    if not data or data.get("Status") != "ok":
        return ""
    results = data.get("Result", [])
    return (results[0].get("OcrText", "") or "") if results else ""


def bhl_get_best_ocr(item_id, job_id=None, bhl_key=None):
    text = bhl_get_ocr_via_ia(item_id, job_id, bhl_key=bhl_key)
    if text:
        return text
    data = bhl_get({"op": "GetItemMetadata", "id": item_id,
                    "pages": "true", "ocr": "false"}, job_id, api_key=bhl_key)
    if not data or data.get("Status") != "ok":
        return ""
    results = data.get("Result", [])
    if not results:
        return ""
    page_ids = [str(p["PageID"]) for p in results[0].get("Pages", [])[:MAX_OCR_PAGES]
                if p.get("PageID")]
    texts = []
    for pid in page_ids:
        t = bhl_get_page_ocr(pid, job_id, bhl_key=bhl_key)
        if t:
            texts.append(t)
        time.sleep(BHL_RATE_LIMIT)
    return "\n".join(texts)


# ══════════════════════════════════════════════════════════════════════════════
# GBIF / INAT TAXON RESOLUTION
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/taxon/suggest")
def taxon_suggest():
    q = request.args.get("q", "").strip()
    if not q or len(q) < 2:
        return jsonify([])
    try:
        r = req_lib.get(f"{GBIF_BASE}/species/suggest",
                        params={"q": q, "limit": 10}, timeout=10)
        r.raise_for_status()
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/taxon/resolve", methods=["POST"])
def taxon_resolve():
    """Resolve a taxon key and return order, common name, predator query terms."""
    data = request.get_json(force=True)
    taxon_key = data.get("taxon_key")
    taxon_name = data.get("taxon_name", "")

    if not taxon_key and not taxon_name:
        return jsonify({"error": "taxon_key or taxon_name required"}), 400

    result = {
        "canonical_name": taxon_name,
        "order": "",
        "order_common": "",
        "taxon_common": "",
        "rank": "",
        "prey_order_term": "prey Hymenoptera",
        "prey_common_term": "prey bees",
    }

    try:
        # Get GBIF species info
        if taxon_key:
            r = req_lib.get(f"{GBIF_BASE}/species/{taxon_key}", timeout=10)
        else:
            r = req_lib.get(f"{GBIF_BASE}/species/match",
                            params={"name": taxon_name, "verbose": "true"}, timeout=10)
        if r.status_code == 200:
            info = r.json()
            result["canonical_name"] = info.get("canonicalName") or info.get("scientificName") or taxon_name
            result["order"] = info.get("order", "")
            result["rank"]  = info.get("rank", "")
            result["family"] = info.get("family", "")

        # Get common name from iNaturalist
        inat_name = result["canonical_name"] or taxon_name
        try:
            ir = req_lib.get(f"{INAT_BASE}/taxa",
                             params={"q": inat_name, "per_page": 1, "locale": "en"},
                             timeout=10)
            if ir.status_code == 200:
                hits = ir.json().get("results", [])
                if hits:
                    common = hits[0].get("preferred_common_name", "")
                    if not common:
                        common = hits[0].get("english_common_name", "")
                    result["taxon_common"] = common or ""
        except Exception:
            pass

        # Determine prey terms dynamically from order
        order = (result.get("order") or "").lower()
        ORDER_MAP = {
            "hymenoptera":  ("prey Hymenoptera", "prey bees"),
            "lepidoptera":  ("prey Lepidoptera", "prey butterflies"),
            "coleoptera":   ("prey Coleoptera",  "prey beetles"),
            "diptera":      ("prey Diptera",      "prey flies"),
            "hemiptera":    ("prey Hemiptera",    "prey bugs"),
            "orthoptera":   ("prey Orthoptera",   "prey grasshoppers"),
            "araneae":      ("prey Araneae",      "prey spiders"),
            "neuroptera":   ("prey Neuroptera",   "prey lacewings"),
            "odonata":      ("prey Odonata",      "prey dragonflies"),
            "blattodea":    ("prey Blattodea",    "prey cockroaches"),
            "mantodea":     ("prey Mantodea",     "prey mantises"),
            "phasmatodea":  ("prey Phasmatodea",  "prey stick insects"),
            "isoptera":     ("prey Isoptera",     "prey termites"),
            "thysanoptera": ("prey Thysanoptera", "prey thrips"),
            "psocoptera":   ("prey Psocoptera",   "prey booklice"),
            "ephemeroptera":("prey Ephemeroptera","prey mayflies"),
            "plecoptera":   ("prey Plecoptera",   "prey stoneflies"),
            "trichoptera":  ("prey Trichoptera",  "prey caddisflies"),
            "mecoptera":    ("prey Mecoptera",    "prey scorpionflies"),
            "siphonaptera": ("prey Siphonaptera", "prey fleas"),
            "phthiraptera": ("prey Phthiraptera", "prey lice"),
        }
        if order in ORDER_MAP:
            result["prey_order_term"], result["prey_common_term"] = ORDER_MAP[order]
        elif order:
            # Capitalize order for the query
            ord_cap = order.capitalize()
            result["prey_order_term"]  = f"prey {ord_cap}"
            # Use common name of taxon if available, else use order
            common_fallback = result.get("taxon_common") or ord_cap.lower()
            result["prey_common_term"] = f"prey {common_fallback}"

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify(result)


# ══════════════════════════════════════════════════════════════════════════════
# AI CLASSIFICATION (OpenRouter)
# ══════════════════════════════════════════════════════════════════════════════

OPENROUTER_BASE = "https://openrouter.ai/api/v1"

@app.route("/api/ai/classify", methods=["POST"])
def ai_classify():
    """
    Use an OpenRouter LLM to classify a taxon and determine smart prey/host terms.
    Accepts: { taxon_name, openrouter_key, model (optional) }
    Returns: { animal_type, prey_order_term, prey_common_term, rationale,
               is_parasite, is_predator, is_kleptoparasite, notes }
    """
    data = request.get_json(force=True)
    taxon_name    = (data.get("taxon_name") or "").strip()
    or_key        = (data.get("openrouter_key") or "").strip()
    model         = (data.get("model") or "openai/gpt-4o-mini:free").strip()

    if not taxon_name:
        return jsonify({"error": "taxon_name is required"}), 400
    if not or_key:
        return jsonify({"error": "openrouter_key is required"}), 400

    system_prompt = (
        "You are an expert entomologist and ecologist. "
        "Given a taxon name (genus, family, or species), classify it and determine the best "
        "literature search query terms to use when searching for papers about its relationship "
        "with Melissodes bees (or bees in general). "
        "Respond ONLY with a valid JSON object, no markdown, no extra text."
    )

    user_prompt = f"""Taxon: {taxon_name}

Please return a JSON object with these exact fields:
{{
  "animal_type": "<brief description, e.g. 'spider wasp', 'bee fly', 'solitary wasp', 'mite', 'fungus gnat'>",
  "is_predator": true/false,
  "is_parasite": true/false,
  "is_kleptoparasite": true/false,
  "prey_order_term": "<the best 'prey X' search term using taxonomic order, e.g. 'prey Hymenoptera'>",
  "prey_common_term": "<the best 'prey X' search term using common name, e.g. 'prey bees'>",
  "host_term": "<if parasitic: 'host bees' or similar; else empty string>",
  "rationale": "<1-2 sentence explanation of why these terms were chosen>",
  "notes": "<any caveats or additional context, e.g. if the taxon has multiple feeding strategies>"
}}

Key rules:
- If the taxon is a predator of bees (e.g. Philanthus, Cerceris), use prey terms like 'prey bees'/'prey Hymenoptera'.
- If it is a parasite/parasitoid of bees (e.g. Melittobia, Strepsiptera), use 'host bees' and set is_parasite=true.
- If it is a kleptoparasite (e.g. Nomada, Coelioxys), use 'host bees' and set is_kleptoparasite=true.
- If the taxon preys on a different insect order (e.g. Lepidoptera), use the correct order in prey_order_term.
- If uncertain about the relationship to bees, still provide best-guess terms.
"""

    try:
        r = req_lib.post(
            f"{OPENROUTER_BASE}/chat/completions",
            headers={
                "Authorization": f"Bearer {or_key}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://literature-miner.hf.space",
                "X-Title": "Literature Miner",
            },
            json={
                "model": model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_prompt},
                ],
                "temperature": 0.1,
                "max_tokens": 512,
            },
            timeout=30,
        )
        r.raise_for_status()
        content = r.json()["choices"][0]["message"]["content"].strip()
        # Strip markdown fences if present
        content = re.sub(r"^```(?:json)?\s*", "", content)
        content = re.sub(r"\s*```$", "", content).strip()
        result = json.loads(content)
        # Ensure required fields exist with defaults
        result.setdefault("prey_order_term", "prey Hymenoptera")
        result.setdefault("prey_common_term", "prey bees")
        result.setdefault("host_term", "")
        result.setdefault("animal_type", "unknown")
        result.setdefault("is_predator", False)
        result.setdefault("is_parasite", False)
        result.setdefault("is_kleptoparasite", False)
        result.setdefault("rationale", "")
        result.setdefault("notes", "")
        return jsonify(result)
    except req_lib.exceptions.HTTPError as e:
        return jsonify({"error": f"OpenRouter HTTP error: {e.response.status_code} — {e.response.text[:200]}"}), 502
    except json.JSONDecodeError as e:
        return jsonify({"error": f"AI returned invalid JSON: {str(e)}"}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ══════════════════════════════════════════════════════════════════════════════
# CHECKPOINT SYSTEM (.litminer = ZIP)
# ══════════════════════════════════════════════════════════════════════════════

def build_litminer_zip(job_id):
    """Build a .litminer (zip) file from current job state."""
    job = JOBS.get(job_id)
    if not job:
        return None
    cp = job["checkpoint"]

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        # Main state JSON
        state = {
            "job_id":         job_id,
            "created_at":     time.time(),
            "version":        "2.0",
            "status":         job["status"],
            "taxon_info":     cp.get("taxon_info", {}),
            "query_terms":    cp.get("query_terms", {}),
            "seen_keys":      cp.get("seen_keys", {}),
            "seen_bhl_ids":   cp.get("seen_bhl_ids", {}),
            "direct_hits":    cp.get("direct_hits", []),
            "fulltext_queue": cp.get("fulltext_queue", []),
            "bhl_ocr_queue":  cp.get("bhl_ocr_queue", []),
            "bhl_direct_hits":cp.get("bhl_direct_hits", []),
            "all_rows":       cp.get("all_rows", []),
            "std_done":       cp.get("std_done", []),
            "pred_done":      cp.get("pred_done", []),
            "bhl_std_done":   cp.get("bhl_std_done", []),
            "bhl_pred_done":  cp.get("bhl_pred_done", []),
            "ocr_hits":       cp.get("ocr_hits", []),
            "phase":          cp.get("phase", "search"),
            "progress":       cp.get("progress", {}),
            "stats":          {k: v for k, v in cp.get("stats", {}).items()
                               if k != "req_log"},
        }
        zf.writestr("state.json", json.dumps(state, ensure_ascii=False, separators=(",", ":")))

        # fetched_ocr separately (can be large)
        zf.writestr("fetched_ocr.json",
                    json.dumps(cp.get("fetched_ocr", {}),
                               ensure_ascii=False, separators=(",", ":")))

        # Current CSV
        if job.get("csv"):
            zf.writestr("results.csv", job["csv"])
        elif cp.get("all_rows"):
            df = pd.DataFrame(cp["all_rows"])
            zf.writestr("results.csv", df.to_csv(index=False))

        # Manifest
        manifest = {
            "format": "litminer",
            "version": "2.0",
            "rows": len(cp.get("all_rows", [])),
            "std_done": len(cp.get("std_done", [])),
            "pred_done": len(cp.get("pred_done", [])),
            "bhl_std_done": len(cp.get("bhl_std_done", [])),
            "bhl_pred_done": len(cp.get("bhl_pred_done", [])),
            "phase": cp.get("phase", "search"),
            "taxon": cp.get("taxon_info", {}).get("canonical_name", "unknown"),
        }
        zf.writestr("manifest.json", json.dumps(manifest))

    buf.seek(0)
    return buf.read()


def load_litminer_zip(data: bytes):
    """Load a .litminer checkpoint, returns dict of state."""
    try:
        buf = io.BytesIO(data)
        with zipfile.ZipFile(buf, "r") as zf:
            state = json.loads(zf.read("state.json"))
            fetched_ocr = {}
            if "fetched_ocr.json" in zf.namelist():
                fetched_ocr = json.loads(zf.read("fetched_ocr.json"))
            state["fetched_ocr"] = fetched_ocr
            return state
    except Exception as e:
        return {"error": str(e)}


# ══════════════════════════════════════════════════════════════════════════════
# DYNAMIC QUERY BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def build_queries(taxa_text: str, taxon_info: dict, query_terms: dict):
    """
    Build mel_queries and pred_queries dynamically based on:
    - taxa_text: newline/comma separated predator taxa list
    - taxon_info: {canonical_name, order, taxon_common, prey_order_term, prey_common_term}
    - query_terms: user-specified extra terms
    """
    raw = re.split(r"[\n,]+", taxa_text)
    taxa = [t.strip().strip("'\"") for t in raw if t.strip()]
    taxa = [t for t in taxa if t]

    target = taxon_info.get("canonical_name", "Melissodes")
    prey_order = query_terms.get("prey_order") or taxon_info.get("prey_order_term", "prey Hymenoptera")
    prey_common = query_terms.get("prey_common") or taxon_info.get("prey_common_term", "prey bees")

    # Extract just the prey object (e.g. "Hymenoptera" from "prey Hymenoptera")
    prey_order_obj  = prey_order.replace("prey ", "").strip()
    prey_common_obj = prey_common.replace("prey ", "").strip()

    mel_queries = (
        [
            f"{target} parasites", f"{target} predators",
            f"{target} kleptoparasites", f"{target} natural enemies",
            f"{target} bee parasitoid", f"{target} nest parasites",
            f"{target} bee predator", f"{target} Strepsiptera stylopization",
            f"{target} bee host parasite", f"{target} bee prey",
        ]
        + [f"{target} {t}" for t in taxa]
    )

    pred_queries = []
    for t in taxa:
        pred_queries.append(f"{t} {prey_order}")
        pred_queries.append(f"{t} {prey_common}")

    bhl_mel_queries = [f"{target} {t}" for t in taxa] + [
        target, f"{target} parasit", f"{target} predator",
    ]
    bhl_pred_queries = []
    for t in taxa:
        bhl_pred_queries.append(f"{t} {prey_order_obj}")
        bhl_pred_queries.append(f"{t} {prey_common_obj}")

    return mel_queries, pred_queries, bhl_mel_queries, bhl_pred_queries


# ══════════════════════════════════════════════════════════════════════════════
# MAIN WORKER
# ══════════════════════════════════════════════════════════════════════════════

def run_job(job_id: str, cfg: dict, taxa_text: str, resume_state: dict = None):
    job = JOBS[job_id]
    job["status"]       = "running"
    job["queue_status"] = "running"
    cp  = job["checkpoint"]

    def log(msg, level="info"):
        job_log(job_id, msg, level)

    def milestone(msg):
        """Emit a log entry tagged as a checkpoint milestone for frontend notifications."""
        job_log(job_id, msg, "milestone")

    def update_progress(**kwargs):
        cp["progress"].update(kwargs)

    def is_stopped():
        return job.get("stop_flag", False)

    def save_inline_checkpoint(label=""):
        """Update in-memory checkpoint; download triggered by user anytime."""
        cp["stats"]["total_hits"] = len(cp["all_rows"])
        cp["stats"]["check_hits"] = sum(1 for r in cp["all_rows"]
                                        if r.get("pre_verdict") == "CHECK")
        cp["stats"]["noise_hits"] = sum(1 for r in cp["all_rows"]
                                        if r.get("pre_verdict") == "NOISE")
        # Also update CSV
        if cp["all_rows"]:
            df = pd.DataFrame(cp["all_rows"])
            job["csv"] = df.to_csv(index=False)
        if label:
            log(f"💾 Checkpoint saved: {label}", "checkpoint")

    try:
        # ── Resume from checkpoint if provided ────────────────────────────────
        if resume_state:
            log("♻  Restoring from .litminer checkpoint…", "checkpoint")
            for key in ["seen_keys","seen_bhl_ids","direct_hits","fulltext_queue",
                        "bhl_ocr_queue","bhl_direct_hits","all_rows","std_done",
                        "pred_done","bhl_std_done","bhl_pred_done","ocr_hits",
                        "fetched_ocr","phase","taxon_info","query_terms","progress"]:
                if key in resume_state:
                    cp[key] = resume_state[key]
            log(f"  ↩  Phase: {cp['phase']} | Rows: {len(cp['all_rows'])} | "
                f"Std done: {len(cp['std_done'])} | Pred done: {len(cp['pred_done'])}", "checkpoint")

        # ── Taxon info ────────────────────────────────────────────────────────
        taxon_info  = cp.get("taxon_info") or cfg.get("taxon_info", {})
        query_terms = cp.get("query_terms") or cfg.get("query_terms", {})
        cp["taxon_info"]  = taxon_info
        cp["query_terms"] = query_terms
        target = taxon_info.get("canonical_name", "Melissodes")
        target_lower = target.lower()

        log(f"🔬 Target taxon: {target}", "info")
        log(f"   Order: {taxon_info.get('order','?')} | "
            f"Common: {taxon_info.get('taxon_common','?')}", "info")
        log(f"   Prey terms: [{query_terms.get('prey_order') or taxon_info.get('prey_order_term','?')}] "
            f"/ [{query_terms.get('prey_common') or taxon_info.get('prey_common_term','?')}]", "info")

        log("⚙  Building query lists…")
        mel_queries, pred_queries, bhl_mel_queries, bhl_pred_queries = build_queries(
            taxa_text, taxon_info, query_terms
        )
        log(f"📋 Std: {len(mel_queries)} | Pred: {len(pred_queries)} | "
            f"BHL-std: {len(bhl_mel_queries)} | BHL-pred: {len(bhl_pred_queries)}")

        update_progress(
            std_total=len(mel_queries), pred_total=len(pred_queries),
            bhl_std_total=len(bhl_mel_queries), bhl_pred_total=len(bhl_pred_queries),
        )

        # Sets for skipping done items
        std_done_set      = set(cp.get("std_done", []))
        pred_done_set     = set(cp.get("pred_done", []))
        bhl_std_done_set  = set(cp.get("bhl_std_done", []))
        bhl_pred_done_set = set(cp.get("bhl_pred_done", []))

        seen_keys    = cp.get("seen_keys", {})
        fulltext_queue = cp.get("fulltext_queue", [])
        direct_hits  = cp.get("direct_hits", [])
        all_rows     = cp.get("all_rows", [])
        skipped_blocked = 0
        skipped_no_bees = 0

        def add_record(rec, query, query_type):
            nonlocal skipped_blocked, skipped_no_bees
            title    = rec.get("title", "")
            abstract = rec.get("abstract", "")
            doi      = rec.get("doi", "")
            year     = rec.get("year", "")

            if _title_is_hard_blocked(title):
                skipped_blocked += 1
                return

            dk = _dedup_key(doi, title, year)
            if dk in seen_keys:
                return
            seen_keys[dk] = True
            rec["query"]      = query
            rec["query_type"] = query_type

            if query_type == "predator_centric":
                if not (target_lower in (title + abstract).lower() or
                        _has_bee_signal(title, abstract)):
                    skipped_no_bees += 1
                    return
                if abstract and target_lower in abstract.lower():
                    results = extract_hits_predator(abstract, title, target_term=target_lower)
                    if results:
                        for trigger, kw, pv, nr, mc in results:
                            row = {**rec, "query_type": "predator_centric",
                                   "matched_keyword": kw, "mel_count": mc,
                                   "pre_verdict": pv, "noise_reason": nr,
                                   "trigger": trigger, "taxon_target": target}
                            direct_hits.append(row)
                            all_rows.append(row)
                        return
                fulltext_queue.append(rec)
                return

            if abstract:
                result = check_abstract_snippet(abstract, title, target_lower)
                if result:
                    trigger, kw, pv, nr = result
                    row = {**rec, "query_type": "standard",
                           "matched_keyword": kw, "mel_count": 1,
                           "pre_verdict": pv, "noise_reason": nr,
                           "trigger": trigger, "taxon_target": target}
                    direct_hits.append(row)
                    all_rows.append(row)
                    return
            if rec.get("oa_url") or any(d in rec.get("url", "") for d in OA_DOMAINS):
                fulltext_queue.append(rec)

        # ── PHASE 1: Standard literature search ────────────────────────────
        if cp.get("phase", "search") in ("search",):
            log("─" * 50)
            log(f"STEP 1 — Literature Search (Academic APIs)")

            std_todo = [q for q in mel_queries if q not in std_done_set]
            log(f"  Std queries: {len(std_todo)} to run ({len(std_done_set)} already done)")
            update_progress(std_done=len(std_done_set))

            for i, q in enumerate(std_todo, 1):
                if is_stopped():
                    log("⏹ Stopped by user", "warn")
                    save_inline_checkpoint("user stop — phase 1 std")
                    job["status"] = "stopped"
                    return
                if i % 5 == 1:
                    log(f"  Std {i}/{len(std_todo)} — hits: {len(direct_hits)} | queued: {len(fulltext_queue)}")
                for rec in search_all_sources(q, cfg, job_id):
                    add_record(rec, q, "standard")
                std_done_set.add(q)
                cp["std_done"] = list(std_done_set)
                update_progress(std_done=len(std_done_set))
                if i % CHECKPOINT_INTERVAL_STD == 0:
                    cp["seen_keys"] = seen_keys
                    cp["direct_hits"] = direct_hits
                    cp["fulltext_queue"] = fulltext_queue
                    cp["all_rows"] = all_rows
                    save_inline_checkpoint(f"std query {i}/{len(std_todo)}")

            log(f"✓ Standard queries done. Hits: {len(direct_hits)} | FT queue: {len(fulltext_queue)}")

            pred_todo = [q for q in pred_queries if q not in pred_done_set]
            log(f"  Pred queries: {len(pred_todo)} to run ({len(pred_done_set)} already done)")
            update_progress(pred_done=len(pred_done_set))

            for i, q in enumerate(pred_todo, 1):
                if is_stopped():
                    log("⏹ Stopped by user", "warn")
                    save_inline_checkpoint("user stop — phase 1 pred")
                    job["status"] = "stopped"
                    return
                if i % 50 == 1:
                    log(f"  Pred {i}/{len(pred_todo)} — hits: {len(direct_hits)} | queued: {len(fulltext_queue)}")
                for rec in search_all_sources(q, cfg, job_id):
                    add_record(rec, q, "predator_centric")
                pred_done_set.add(q)
                cp["pred_done"] = list(pred_done_set)
                update_progress(pred_done=len(pred_done_set))
                if i % CHECKPOINT_INTERVAL_PRED == 0:
                    cp["seen_keys"] = seen_keys
                    cp["direct_hits"] = direct_hits
                    cp["fulltext_queue"] = fulltext_queue
                    cp["all_rows"] = all_rows
                    save_inline_checkpoint(f"pred query {i}/{len(pred_todo)}")

            log(f"✓ Pred queries done. Hits: {len(direct_hits)} | FT queue: {len(fulltext_queue)}")

            # Sync checkpoint
            cp["seen_keys"]     = seen_keys
            cp["direct_hits"]   = direct_hits
            cp["fulltext_queue"] = fulltext_queue
            cp["all_rows"]      = all_rows
            save_inline_checkpoint("Phase 1 complete")
            milestone(f"📍 Phase 1 complete — {len(all_rows)} hits so far. Download checkpoint now if needed.")
            cp["phase"] = "fulltext"

        # ── PHASE 2: Full-text scan ────────────────────────────────────────
        if cp.get("phase") in ("fulltext",) and FETCH_FULLTEXT:
            log("─" * 50)
            fq = cp.get("fulltext_queue", [])
            log(f"STEP 2 — Full-text scan ({len(fq)} records)")
            update_progress(ft_total=len(fq))
            ft_hits = []
            fetched_urls = {}
            skipped_no_oa = skipped_no_mel_ft = 0

            for i, rec in enumerate(fq, 1):
                if is_stopped():
                    log("⏹ Stopped by user", "warn")
                    save_inline_checkpoint("user stop — phase 2 fulltext")
                    job["status"] = "stopped"
                    return
                if i % 50 == 0:
                    log(f"  FT {i}/{len(fq)} — hits: {len(ft_hits)}")
                    update_progress(ft_done=i)
                title   = rec.get("title", "")
                oa_url  = rec.get("oa_url", "")
                url     = rec.get("url", "")
                is_pred = rec.get("query_type") == "predator_centric"
                cache_key = oa_url or url
                if cache_key not in fetched_urls:
                    fetched_urls[cache_key] = fetch_fulltext(oa_url, url)
                text = fetched_urls[cache_key]
                if not text:
                    skipped_no_oa += 1
                    continue
                if target_lower not in text.lower():
                    skipped_no_mel_ft += 1
                    continue
                doi  = rec.get("doi", "")
                year = rec.get("year", "")
                if is_pred:
                    for trigger, kw, pv, nr, mc in extract_hits_predator(text, title, target_term=target_lower):
                        row = {**rec, "query_type": "predator_centric",
                               "matched_keyword": kw, "mel_count": mc,
                               "pre_verdict": pv, "noise_reason": nr,
                               "trigger": trigger, "taxon_target": target}
                        ft_hits.append(row); all_rows.append(row)
                else:
                    for trigger, kw, pv, nr in extract_hits_standard(text, title, target_lower):
                        row = {**rec, "query_type": "standard",
                               "matched_keyword": kw, "mel_count": 1,
                               "pre_verdict": pv, "noise_reason": nr,
                               "trigger": trigger, "taxon_target": target}
                        ft_hits.append(row); all_rows.append(row)

            log(f"✓ Full-text done. New hits: {len(ft_hits)} | No-OA: {skipped_no_oa} | No-match: {skipped_no_mel_ft}")
            cp["all_rows"] = all_rows
            save_inline_checkpoint("Phase 2 complete")
            milestone(f"📍 Phase 2 complete — full-text scan done. {len(all_rows)} total hits. Download checkpoint now if needed.")
            cp["phase"] = "bhl_search"

        # ── PHASE 3: BHL Search ────────────────────────────────────────────
        if cp.get("phase") in ("bhl_search",) and cfg.get("sources", {}).get("bhl", True):
            log("─" * 50)
            log("STEP 3 — BHL (Biodiversity Heritage Library) Search")

            seen_bhl_ids   = cp.get("seen_bhl_ids", {})
            bhl_ocr_queue  = cp.get("bhl_ocr_queue", [])
            bhl_direct_hits= cp.get("bhl_direct_hits", [])

            bhl_std_todo  = [q for q in bhl_mel_queries  if q not in bhl_std_done_set]
            bhl_pred_todo = [q for q in bhl_pred_queries if q not in bhl_pred_done_set]
            log(f"  BHL std: {len(bhl_std_todo)} | BHL pred: {len(bhl_pred_todo)}")
            update_progress(bhl_std_total=len(bhl_std_todo)+len(bhl_std_done_set),
                            bhl_pred_total=len(bhl_pred_todo)+len(bhl_pred_done_set))

            def bhl_add_record(bhl_rec):
                url     = bhl_rec.get("url", "")
                bhl_id  = (bhl_rec.get("part_id") or bhl_rec.get("item_id") or
                           bhl_rec.get("page_id") or url)
                if bhl_id in seen_bhl_ids:
                    return
                seen_bhl_ids[bhl_id] = True
                title   = bhl_rec.get("title", "")
                snippet = bhl_rec.get("snippet", "")
                query_type = bhl_rec.get("query_type", "standard")

                if _title_is_hard_blocked(title):
                    return

                if query_type == "predator_centric":
                    if not _has_bee_signal(title, snippet):
                        return
                    bhl_ocr_queue.append(bhl_rec)
                    return

                if snippet:
                    result = check_abstract_snippet(snippet, title, target_lower)
                    if result:
                        trigger, kw, pv, nr = result
                        row = {
                            "title": title, "author": bhl_rec.get("author",""),
                            "year": bhl_rec.get("year",""), "url": url,
                            "doi": "", "source": "bhl",
                            "source_id": bhl_id, "query": bhl_rec.get("query",""),
                            "query_type": "standard_bhl",
                            "matched_keyword": kw, "mel_count": 1,
                            "pre_verdict": pv, "noise_reason": nr,
                            "trigger": trigger, "taxon_target": target,
                        }
                        bhl_direct_hits.append(row)
                        all_rows.append(row)
                        return
                bhl_ocr_queue.append(bhl_rec)

            for i, q in enumerate(bhl_std_todo, 1):
                if is_stopped():
                    log("⏹ Stopped by user", "warn")
                    cp["seen_bhl_ids"] = seen_bhl_ids
                    cp["bhl_ocr_queue"] = bhl_ocr_queue
                    cp["bhl_direct_hits"] = bhl_direct_hits
                    cp["all_rows"] = all_rows
                    save_inline_checkpoint("user stop — BHL std")
                    job["status"] = "stopped"
                    return
                if i % 5 == 1:
                    log(f"  BHL-std {i}/{len(bhl_std_todo)} — direct: {len(bhl_direct_hits)} | queue: {len(bhl_ocr_queue)}")
                for rec in bhl_run_query(q, "standard", job_id, bhl_key=cfg.get("bhl_key")): 
                    bhl_add_record(rec)
                bhl_std_done_set.add(q)
                cp["bhl_std_done"] = list(bhl_std_done_set)
                update_progress(bhl_std_done=len(bhl_std_done_set))

            for i, q in enumerate(bhl_pred_todo, 1):
                if is_stopped():
                    log("⏹ Stopped by user", "warn")
                    cp["seen_bhl_ids"] = seen_bhl_ids
                    cp["bhl_ocr_queue"] = bhl_ocr_queue
                    cp["bhl_direct_hits"] = bhl_direct_hits
                    cp["all_rows"] = all_rows
                    save_inline_checkpoint("user stop — BHL pred")
                    job["status"] = "stopped"
                    return
                if i % 50 == 1:
                    log(f"  BHL-pred {i}/{len(bhl_pred_todo)} — direct: {len(bhl_direct_hits)} | queue: {len(bhl_ocr_queue)}")
                for rec in bhl_run_query(q, "predator_centric", job_id, bhl_key=cfg.get("bhl_key")):
                    bhl_add_record(rec)
                bhl_pred_done_set.add(q)
                cp["bhl_pred_done"] = list(bhl_pred_done_set)
                update_progress(bhl_pred_done=len(bhl_pred_done_set))

            cp["seen_bhl_ids"]    = seen_bhl_ids
            cp["bhl_ocr_queue"]   = bhl_ocr_queue
            cp["bhl_direct_hits"] = bhl_direct_hits
            cp["all_rows"]        = all_rows
            log(f"✓ BHL search done. Direct hits: {len(bhl_direct_hits)} | OCR queue: {len(bhl_ocr_queue)}")
            save_inline_checkpoint("Phase 3 — BHL search complete")
            milestone(f"📍 Phase 3 complete — BHL search done. {len(bhl_direct_hits)} BHL hits, {len(bhl_ocr_queue)} queued for OCR. Download checkpoint now if needed.")
            cp["phase"] = "bhl_ocr"

        # ── PHASE 4: BHL OCR scan ──────────────────────────────────────────
        if cp.get("phase") in ("bhl_ocr",) and cfg.get("sources", {}).get("bhl", True):
            log("─" * 50)
            ocr_work = cp.get("bhl_ocr_queue", [])
            log(f"STEP 4 — BHL OCR Scan ({len(ocr_work)} records)")
            update_progress(ocr_total=len(ocr_work))
            ocr_hits    = cp.get("ocr_hits", [])
            fetched_ocr = cp.get("fetched_ocr", {})
            skipped_no_ocr = skipped_no_mel_ocr = 0

            for ocr_i, rec in enumerate(ocr_work, 1):
                if is_stopped():
                    log("⏹ Stopped by user", "warn")
                    cp["ocr_hits"]   = ocr_hits
                    cp["fetched_ocr"]= fetched_ocr
                    cp["all_rows"]   = all_rows
                    save_inline_checkpoint("user stop — BHL OCR")
                    job["status"] = "stopped"
                    return
                if ocr_i % 10 == 1:
                    log(f"  OCR {ocr_i}/{len(ocr_work)} — hits: {len(ocr_hits)}")
                    update_progress(ocr_done=ocr_i)

                is_part  = bool(rec.get("part_id"))
                rec_id   = rec.get("part_id") or rec.get("item_id")
                is_pred  = rec.get("query_type") == "predator_centric"
                title    = rec.get("title", "")

                if is_part:
                    item_id = bhl_get_part_item_id(rec["part_id"], job_id, bhl_key=cfg.get("bhl_key"))
                    time.sleep(BHL_RATE_LIMIT)
                    if not item_id:
                        skipped_no_ocr += 1
                        continue
                else:
                    item_id = rec.get("item_id")
                    if not item_id:
                        skipped_no_ocr += 1
                        continue

                if item_id not in fetched_ocr:
                    fetched_ocr[item_id] = bhl_get_best_ocr(item_id, job_id, bhl_key=cfg.get("bhl_key"))

                ocr_text = fetched_ocr[item_id]
                if not ocr_text:
                    skipped_no_ocr += 1
                    continue
                if target_lower not in ocr_text.lower():
                    skipped_no_mel_ocr += 1
                    continue

                if is_pred:
                    for trigger, kw, pv, nr, mc in extract_hits_predator(ocr_text, title, target_term=target_lower):
                        row = {
                            "title": title, "author": rec.get("author",""),
                            "year": rec.get("year",""), "url": rec.get("url",""),
                            "doi": "", "source": "bhl_ocr",
                            "source_id": rec_id, "query": rec.get("query",""),
                            "query_type": "predator_centric_bhl",
                            "matched_keyword": kw, "mel_count": mc,
                            "pre_verdict": pv, "noise_reason": nr,
                            "trigger": trigger, "taxon_target": target,
                        }
                        ocr_hits.append(row); all_rows.append(row)
                else:
                    for trigger, kw, pv, nr in extract_hits_standard(ocr_text, title, target_lower):
                        row = {
                            "title": title, "author": rec.get("author",""),
                            "year": rec.get("year",""), "url": rec.get("url",""),
                            "doi": "", "source": "bhl_ocr",
                            "source_id": rec_id, "query": rec.get("query",""),
                            "query_type": "standard_bhl",
                            "matched_keyword": kw, "mel_count": 1,
                            "pre_verdict": pv, "noise_reason": nr,
                            "trigger": trigger, "taxon_target": target,
                        }
                        ocr_hits.append(row); all_rows.append(row)

                if ocr_i % CHECKPOINT_INTERVAL_OCR == 0:
                    cp["ocr_hits"]    = ocr_hits
                    cp["fetched_ocr"] = fetched_ocr
                    cp["all_rows"]    = all_rows
                    save_inline_checkpoint(f"OCR {ocr_i}/{len(ocr_work)}")

            cp["ocr_hits"]    = ocr_hits
            cp["fetched_ocr"] = fetched_ocr
            cp["all_rows"]    = all_rows
            log(f"✓ OCR done. New hits: {len(ocr_hits)} | No-OCR: {skipped_no_ocr} | No-match: {skipped_no_mel_ocr}")
            save_inline_checkpoint("Phase 4 — BHL OCR complete")
            milestone(f"📍 Phase 4 complete — OCR scan done. {len(all_rows)} total hits. Download checkpoint now if needed.")
            cp["phase"] = "export"

        # ── PHASE 5: Export ────────────────────────────────────────────────
        log("─" * 50)
        log("STEP 5 — Building results")
        total = len(all_rows)
        check = sum(1 for r in all_rows if r.get("pre_verdict") == "CHECK")
        noise = sum(1 for r in all_rows if r.get("pre_verdict") == "NOISE")
        log(f"  Total rows : {total}")
        log(f"    CHECK    : {check}  (likely relevant)")
        log(f"    NOISE    : {noise}  (likely irrelevant)")

        df  = pd.DataFrame(all_rows) if all_rows else pd.DataFrame()
        csv = df.to_csv(index=False) if not df.empty else ""

        job["rows"]   = all_rows
        job["csv"]    = csv
        job["status"] = "done"
        cp["phase"]   = "done"
        cp["all_rows"] = all_rows
        save_inline_checkpoint("COMPLETE")
        milestone(f"🎉 Job complete! {total} total rows ({check} CHECK, {total-check} NOISE). Download your CSV and checkpoint now.")
        log(f"✅ DONE — {total} rows ready for download.")

    except Exception as exc:
        import traceback
        job["status"]    = "error"
        job["error_msg"] = str(exc)
        log(f"❌ ERROR: {exc}", "error")
        log(traceback.format_exc(), "error")


# ══════════════════════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/ping")
def ping():
    return "OK", 200


@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/start", methods=["POST"])
def start():
    data = request.get_json(force=True)
    email      = (data.get("email") or "").strip()
    s2_key     = (data.get("s2_key") or "").strip()
    pubmed_key = (data.get("pubmed_key") or "").strip()
    bhl_key    = (data.get("bhl_key") or "").strip()

    if not email or "@" not in email:
        return jsonify({"error": "A valid email address is required for OpenAlex polite pool."}), 400

    taxa_text = (data.get("taxa") or "").strip()
    if not taxa_text:
        return jsonify({"error": "No taxa list provided."}), 400

    taxon_info  = data.get("taxon_info", {})
    query_terms = data.get("query_terms", {})

    sources = {
        "openalex":         bool(data.get("src_openalex", True)),
        "crossref":         bool(data.get("src_crossref", True)),
        "semantic_scholar": bool(data.get("src_s2", True)),
        "pubmed":           bool(data.get("src_pubmed", True)),
        "bhl":              bool(data.get("src_bhl", True)),
    }

    cfg = {
        "email": email, "s2_key": s2_key, "pubmed_key": pubmed_key, "bhl_key": bhl_key,
        "sources": sources, "taxon_info": taxon_info, "query_terms": query_terms,
    }

    job_id = new_job()
    JOBS[job_id]["checkpoint"]["taxon_info"]  = taxon_info
    JOBS[job_id]["checkpoint"]["query_terms"] = query_terms

    # Determine queue position before enqueuing
    queue_pos = JOB_QUEUE.qsize() + (1 if ACTIVE_JOB["job_id"] else 0)
    JOB_QUEUE.put((job_id, cfg, taxa_text, None))
    if queue_pos == 0:
        job_log(job_id, "▶ No jobs ahead — starting immediately.", "info")
    else:
        job_log(job_id, f"⏳ Queued at position {queue_pos}. Waiting for current job to finish.", "warn")

    return jsonify({"job_id": job_id, "queue_position": queue_pos})


@app.route("/resume", methods=["POST"])
def resume():
    """Resume from a .litminer checkpoint file."""
    email      = (request.form.get("email") or "").strip()
    s2_key     = (request.form.get("s2_key") or "").strip()
    pubmed_key = (request.form.get("pubmed_key") or "").strip()
    bhl_key    = (request.form.get("bhl_key") or "").strip()
    taxa_text  = (request.form.get("taxa") or "").strip()

    if not email or "@" not in email:
        return jsonify({"error": "Email required."}), 400
    if "checkpoint" not in request.files:
        return jsonify({"error": "No checkpoint file provided."}), 400

    cp_bytes = request.files["checkpoint"].read()
    resume_state = load_litminer_zip(cp_bytes)
    if "error" in resume_state:
        return jsonify({"error": f"Bad checkpoint: {resume_state['error']}"}), 400

    if not taxa_text:
        # Try to recover from checkpoint
        taxon_info = resume_state.get("taxon_info", {})
        taxa_text  = " "  # minimal placeholder

    taxon_info  = resume_state.get("taxon_info", {})
    query_terms = resume_state.get("query_terms", {})

    sources = {
        "openalex":         True,
        "crossref":         True,
        "semantic_scholar": bool(s2_key),
        "pubmed":           True,
        "bhl":              True,
    }
    cfg = {
        "email": email, "s2_key": s2_key, "pubmed_key": pubmed_key, "bhl_key": bhl_key,
        "sources": sources, "taxon_info": taxon_info, "query_terms": query_terms,
    }

    job_id = new_job()
    queue_pos = JOB_QUEUE.qsize() + (1 if ACTIVE_JOB["job_id"] else 0)
    JOB_QUEUE.put((job_id, cfg, taxa_text, resume_state))
    if queue_pos == 0:
        job_log(job_id, "▶ No jobs ahead — resuming immediately.", "info")
    else:
        job_log(job_id, f"⏳ Queued at position {queue_pos}. Waiting for current job to finish.", "warn")
    return jsonify({"job_id": job_id, "queue_position": queue_pos})


@app.route("/stop/<job_id>", methods=["POST"])
def stop_job(job_id):
    if job_id not in JOBS:
        return jsonify({"error": "Unknown job"}), 404
    JOBS[job_id]["stop_flag"] = True
    return jsonify({"ok": True})


@app.route("/stream/<job_id>")
def stream(job_id):
    if job_id not in JOBS:
        return jsonify({"error": "Unknown job"}), 404

    def generate():
        job = JOBS[job_id]
        while True:
            try:
                entry = job["log"].get(timeout=1.0)
                cp    = job["checkpoint"]
                payload = {
                    "log":      entry["msg"],
                    "level":    entry.get("level", "info"),
                    "progress": cp.get("progress", {}),
                    "stats":    {
                        "total_hits": cp["stats"]["total_hits"],
                        "check_hits": cp["stats"]["check_hits"],
                        "noise_hits": cp["stats"]["noise_hits"],
                        "api_calls":  cp["stats"]["api_calls"],
                    },
                    "req_log":  cp["stats"].get("req_log", [])[-10:],
                    "phase":    cp.get("phase", ""),
                    "is_milestone": entry.get("level") == "milestone",
                    "queue_status": job.get("queue_status", ""),
                }
                yield f"data: {json.dumps(payload)}\n\n"
            except queue.Empty:
                if job["status"] in ("done", "error", "stopped"):
                    cp = job["checkpoint"]
                    yield f"data: {json.dumps({'status': job['status'], 'row_count': len(job['rows']), 'progress': cp.get('progress',{}), 'stats': cp.get('stats', {})})}\n\n"
                    break
                with QUEUE_LOCK:
                    active = ACTIVE_JOB["job_id"]
                queue_depth = JOB_QUEUE.qsize()
                yield f"data: {json.dumps({'heartbeat': True, 'progress': JOBS[job_id]['checkpoint'].get('progress',{}), 'stats': {k: v for k,v in JOBS[job_id]['checkpoint'].get('stats',{}).items() if k!='req_log'}, 'queue_status': job.get('queue_status',''), 'active_job': active, 'queue_depth': queue_depth})}\n\n"

    return Response(stream_with_context(generate()),
                    mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/status/<job_id>")
def status(job_id):
    if job_id not in JOBS:
        return jsonify({"error": "Unknown job"}), 404
    job = JOBS[job_id]
    cp  = job["checkpoint"]
    return jsonify({
        "status":    job["status"],
        "row_count": len(job["rows"]),
        "error_msg": job["error_msg"],
        "progress":  cp.get("progress", {}),
        "stats":     {k: v for k, v in cp.get("stats", {}).items() if k != "req_log"},
        "phase":     cp.get("phase", ""),
    })


@app.route("/download/csv/<job_id>")
def download_csv(job_id):
    if job_id not in JOBS:
        return jsonify({"error": "Unknown job"}), 404
    job = JOBS[job_id]
    if not job.get("csv") and job["checkpoint"].get("all_rows"):
        df = pd.DataFrame(job["checkpoint"]["all_rows"])
        job["csv"] = df.to_csv(index=False)
    if not job.get("csv"):
        return jsonify({"error": "No results yet"}), 200
    taxon = job["checkpoint"].get("taxon_info", {}).get("canonical_name", "LitMiner")
    fname = re.sub(r"[^\w]", "_", taxon) + "_hits.csv"
    return Response(job["csv"], mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={fname}"})


@app.route("/download/checkpoint/<job_id>")
def download_checkpoint(job_id):
    """Download current state as a .litminer file at any time."""
    if job_id not in JOBS:
        return jsonify({"error": "Unknown job"}), 404
    data = build_litminer_zip(job_id)
    if data is None:
        return jsonify({"error": "Could not build checkpoint"}), 500
    taxon = JOBS[job_id]["checkpoint"].get("taxon_info", {}).get("canonical_name", "LitMiner")
    fname = re.sub(r"[^\w]", "_", taxon) + "_checkpoint.litminer"
    return Response(data, mimetype="application/zip",
                    headers={"Content-Disposition": f"attachment; filename={fname}"})


@app.route("/queue")
def queue_status_route():
    """Return current queue depth and active job id."""
    with QUEUE_LOCK:
        active = ACTIVE_JOB["job_id"]
    depth = JOB_QUEUE.qsize()
    return jsonify({
        "active_job": active,
        "queue_depth": depth,
        "total_waiting": depth,
    })


if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=7860)