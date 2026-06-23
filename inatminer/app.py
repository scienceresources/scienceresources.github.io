"""
iNaturalist Predation/Parasitism Observation Miner
Flask backend — designed for Hugging Face Spaces deployment.
"""

import csv
import io
import json
import queue
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
# Each job: { status, rows, log_queue, total, done, error, taxon_name, taxon_id }

# ─── iNaturalist helpers ──────────────────────────────────────────────────────

INTERACTION_TERMS = [
    "predation", "predator", "prey", "eaten", "eating", "killed", "kill",
    "parasite", "parasitoid", "cuckoo", "mite", "mites", "attacked",
    "attack", "captured", "caught", "stolen", "cleptoparasite", "kleptoparasite",
    "host", "phoresy", "phoretic", "nest", "brood", "larval",
]

TARGET_KEYWORDS = ["melissodes", "eucerini", "long-horned bee", "eucera", "long horned bee"]

MELISSODES_ID = 52781  # iNaturalist taxon ID for Melissodes


def inaturalist_search(taxon_id: int, term: str, per_page: int = 50) -> list:
    """Raw iNaturalist API call — returns list of observation dicts."""
    url = "https://api.inaturalist.org/v1/observations"
    params = {
        "taxon_id": taxon_id,
        "q": term,
        "quality_grade": "casual,needs_id,research",
        "per_page": per_page,
        "fields": "id,description,ofvs,tags,photos,identifications,taxon,user,created_at,place_guess",
    }
    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        return r.json().get("results", [])
    except Exception as e:
        return []


def parse_observation(obs: dict, taxon_id: int, taxon_name: str, term: str) -> dict | None:
    """
    Parse a single iNat observation dict into a record dict,
    applying interaction-evidence and target-validation filters.
    Returns None if the observation should be excluded.
    """
    obs_id = obs.get("id")

    # ── Structured observation fields ────────────────────────────────────────
    ofvs = obs.get("ofvs", [])
    structured = []
    for ofv in ofvs:
        field_name = str(ofv.get("name", "")).lower()
        field_value = str(ofv.get("value", ""))
        if any(kw in field_name for kw in ["eat", "prey", "kill", "interaction", "parasite", "host", "attack"]):
            structured.append(f"{ofv.get('name')}: {field_value}")

    # ── Tags ─────────────────────────────────────────────────────────────────
    tags = [str(t).lower() for t in obs.get("tags", [])]
    matching_tags = [t for t in tags if term in t]

    # ── Description ──────────────────────────────────────────────────────────
    description = str(obs.get("description", "") or "").lower()
    in_description = term in description

    has_interaction_evidence = bool(structured or matching_tags or in_description)

    # ── Target validation for non-Melissodes taxon queries ───────────────────
    is_valid_target = True
    if taxon_id != MELISSODES_ID:
        combined = f"{description} {' '.join(tags)} {' '.join(structured)}"
        direct_match = any(kw in combined for kw in TARGET_KEYWORDS)
        conditional_match = "long antennae" in combined and "bee" in combined
        if not (direct_match or conditional_match):
            is_valid_target = False

    if not (has_interaction_evidence and is_valid_target):
        return None

    # ── Photos ───────────────────────────────────────────────────────────────
    photos = obs.get("photos", []) or []
    photo_urls = []
    for p in photos[:5]:  # cap at 5
        url = p.get("url", "")
        # iNat returns square thumbs; swap for medium
        photo_urls.append(url.replace("square", "medium") if url else "")
    photo_urls = [u for u in photo_urls if u]

    # ── Identifications ───────────────────────────────────────────────────────
    idents = obs.get("identifications", []) or []
    identifiers = []
    for i in idents:
        login = (i.get("user") or {}).get("login", "")
        taxon_label = ((i.get("taxon") or {}).get("name") or "")
        if login or taxon_label:
            identifiers.append(f"{login}: {taxon_label}")

    return {
        "id": obs_id,
        "taxon_queried": taxon_name,
        "keyword": term,
        "url": f"https://www.inaturalist.org/observations/{obs_id}",
        "user": (obs.get("user") or {}).get("login", ""),
        "created_at": obs.get("created_at", "")[:10] if obs.get("created_at") else "",
        "place": obs.get("place_guess", "") or "",
        "quality_grade": obs.get("quality_grade", ""),
        "description": (obs.get("description") or "")[:500],
        "structured_fields": " | ".join(structured) if structured else "",
        "matching_tags": " | ".join(matching_tags) if matching_tags else "",
        "in_description": "yes" if in_description else "no",
        "identifiers": " | ".join(identifiers) if identifiers else "",
        "photo_urls": json.dumps(photo_urls),
        # review fields — set by user
        "review": "",   # yes / no / ambiguous / keep
    }


def mine_worker(job_id: str, taxon_id: int, taxon_name: str, terms: list[str]):
    """Background thread that populates jobs[job_id]."""
    job = jobs[job_id]
    seen_ids: set = set()
    total_terms = len(terms)
    job["total_terms"] = total_terms
    job["status"] = "running"

    def log(msg: str, level: str = "info"):
        job["log_queue"].put({"msg": msg, "level": level, "ts": datetime.utcnow().isoformat()})

    log(f"▶ Starting scan — taxon: {taxon_name} (ID {taxon_id}), {total_terms} keywords", "milestone")

    for i, term in enumerate(terms, 1):
        if job.get("cancel"):
            log("⛔ Job cancelled by user.", "warn")
            break
        log(f"[{i}/{total_terms}] Scanning keyword: '{term}'")
        results = inaturalist_search(taxon_id, term)
        new_count = 0
        for obs in results:
            oid = obs.get("id")
            if oid in seen_ids:
                continue
            record = parse_observation(obs, taxon_id, taxon_name, term)
            if record:
                seen_ids.add(oid)
                job["rows"].append(record)
                new_count += 1
        if new_count:
            log(f"  ✓ +{new_count} records (total: {len(job['rows'])})", "checkpoint")
        job["done_terms"] = i
        time.sleep(1)  # rate-limit courtesy

    job["status"] = "done"
    log(f"✓ Scan complete. {len(job['rows'])} interaction records found.", "milestone")
    job["log_queue"].put({"status": "done", "total": len(job["rows"])})


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.route("/api/taxon/search")
def taxon_search():
    """Proxy iNaturalist taxon autocomplete."""
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify([])
    try:
        r = requests.get(
            "https://api.inaturalist.org/v1/taxa/autocomplete",
            params={"q": q, "per_page": 10, "rank": "genus,species,family,order"},
            timeout=10,
        )
        data = r.json().get("results", [])
        results = []
        for t in data:
            results.append({
                "id": t.get("id"),
                "name": t.get("name"),
                "rank": t.get("rank"),
                "common_name": (t.get("preferred_common_name") or ""),
                "ancestor_ids": t.get("ancestor_ids", []),
            })
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/mine/start", methods=["POST"])
def mine_start():
    data = request.get_json(force=True)
    taxon_id = int(data.get("taxon_id", 0))
    taxon_name = data.get("taxon_name", "Unknown")
    # Accept a newline/comma/semicolon-separated list of additional predator taxa to scan
    predator_list_raw = data.get("predator_list", "")
    custom_terms = data.get("custom_terms", [])  # optional extra keywords

    if not taxon_id:
        return jsonify({"error": "taxon_id required"}), 400

    # Build keyword list
    terms = list(INTERACTION_TERMS)
    if custom_terms:
        terms += [t.strip() for t in custom_terms if t.strip()]

    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {
        "status": "queued",
        "rows": [],
        "log_queue": queue.Queue(),
        "total_terms": len(terms),
        "done_terms": 0,
        "taxon_id": taxon_id,
        "taxon_name": taxon_name,
        "predator_list": predator_list_raw,
        "started_at": datetime.utcnow().isoformat(),
        "cancel": False,
    }

    # If predator list supplied, we'll mine each predator taxon by name→id lookup
    # and check for Melissodes prey references. We kick that off separately.
    thread = threading.Thread(target=mine_worker, args=(job_id, taxon_id, taxon_name, terms), daemon=True)
    thread.start()

    # Additionally mine predator list if provided
    if predator_list_raw.strip():
        pred_thread = threading.Thread(
            target=mine_predator_list_worker,
            args=(job_id, predator_list_raw),
            daemon=True,
        )
        pred_thread.start()

    return jsonify({"job_id": job_id, "taxon_name": taxon_name})


def mine_predator_list_worker(job_id: str, raw_list: str):
    """
    For each taxon name in the predator list, look up its iNat ID,
    then search for observations mentioning Melissodes-related keywords.
    """
    job = jobs[job_id]
    seen_ids_ref = {r["id"] for r in job["rows"]}

    def log(msg, level="info"):
        job["log_queue"].put({"msg": msg, "level": level, "ts": datetime.utcnow().isoformat()})

    names = [n.strip() for n in raw_list.replace(",", "\n").replace(";", "\n").splitlines() if n.strip()]
    log(f"▶ Predator list: {len(names)} taxa to scan", "milestone")

    # Keywords for predator-centric queries (what the predator is doing)
    pred_terms = ["melissodes", "eucerini", "long-horned bee", "prey bee", "host bee", "parasitize"]

    for i, name in enumerate(names, 1):
        if job.get("cancel"):
            break
        # Resolve taxon ID via iNat autocomplete
        try:
            r = requests.get(
                "https://api.inaturalist.org/v1/taxa/autocomplete",
                params={"q": name, "per_page": 1},
                timeout=10,
            )
            hits = r.json().get("results", [])
            if not hits:
                log(f"  [{i}/{len(names)}] '{name}' — no iNat match, skipping", "warn")
                time.sleep(0.5)
                continue
            pred_id = hits[0]["id"]
            pred_name = hits[0]["name"]
        except Exception as e:
            log(f"  [{i}/{len(names)}] '{name}' — lookup error: {e}", "error")
            time.sleep(1)
            continue

        found = 0
        for term in pred_terms:
            results = inaturalist_search(pred_id, term, per_page=30)
            for obs in results:
                oid = obs.get("id")
                if oid in seen_ids_ref:
                    continue
                record = parse_observation(obs, pred_id, pred_name, term)
                if record:
                    seen_ids_ref.add(oid)
                    job["rows"].append(record)
                    found += 1
            time.sleep(0.8)

        if found:
            log(f"  [{i}/{len(names)}] {pred_name} → +{found} records", "checkpoint")
        else:
            log(f"  [{i}/{len(names)}] {pred_name} → no records")
        time.sleep(0.5)

    log(f"✓ Predator list scan complete. Total records: {len(job['rows'])}", "milestone")


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
                item = q.get(timeout=2)
            except queue.Empty:
                # heartbeat
                yield f"data: {json.dumps({'heartbeat': True, 'done_terms': job.get('done_terms', 0), 'total_terms': job.get('total_terms', 1), 'total_rows': len(job['rows']), 'status': job['status']})}\n\n"
                if job["status"] == "done":
                    break
                continue

            payload = {**item, "done_terms": job.get("done_terms", 0), "total_terms": job.get("total_terms", 1), "total_rows": len(job["rows"]), "status": job["status"]}
            yield f"data: {json.dumps(payload)}\n\n"

            if job["status"] == "done" and q.empty():
                break

    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


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
    """Return all discovered rows (for populating review UI)."""
    if job_id not in jobs:
        return jsonify({"error": "not found"}), 404
    return jsonify(jobs[job_id]["rows"])


@app.route("/api/mine/<job_id>/review", methods=["POST"])
def mine_review(job_id):
    """Save a review decision for a single observation."""
    if job_id not in jobs:
        return jsonify({"error": "not found"}), 404
    data = request.get_json(force=True)
    obs_id = data.get("id")
    verdict = data.get("review")  # yes / no / ambiguous / keep
    for row in jobs[job_id]["rows"]:
        if row["id"] == obs_id:
            row["review"] = verdict
            return jsonify({"ok": True})
    return jsonify({"error": "obs not found"}), 404


@app.route("/api/mine/<job_id>/download")
def mine_download(job_id):
    """Download CSV of all rows with review decisions."""
    if job_id not in jobs:
        return jsonify({"error": "not found"}), 404
    rows = jobs[job_id]["rows"]
    if not rows:
        return jsonify({"error": "no data"}), 400

    df = pd.DataFrame(rows)
    # Drop internal json field for cleaner CSV; expand photo URLs
    df["photo_urls"] = df["photo_urls"].apply(lambda x: "; ".join(json.loads(x)) if x else "")
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=inat_interactions_{job_id}.csv"},
    )


@app.route("/api/mine/<job_id>/cancel", methods=["POST"])
def mine_cancel(job_id):
    if job_id not in jobs:
        return jsonify({"error": "not found"}), 404
    jobs[job_id]["cancel"] = True
    return jsonify({"ok": True})


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7860, debug=False)
