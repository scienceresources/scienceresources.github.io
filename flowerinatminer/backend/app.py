"""
Bee-Flower Association Reviewer — Flask backend (API only).

The frontend (index.html) is deployed separately on GitHub Pages, so this
service holds no HTML/static assets — it's a pure JSON API reachable
cross-origin. It holds no state between requests either: it only proxies
two iNaturalist endpoints (taxon autocomplete + observation search) so the
frontend avoids CORS/rate-limit handling itself. All review/classification
decisions and the fetched observation cache live entirely in the browser
(localStorage) on the GitHub Pages side.
"""

import requests
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # allow the GitHub Pages origin (and anywhere else) to call this API

INAT_API = "https://api.inaturalist.org/v1"
PLANTAE_ID = 47126  # iNaturalist taxon ID for Plantae


@app.route("/api/taxon/search")
def taxon_search():
    """
    Proxy iNaturalist taxon autocomplete.
    ?q=            search text (required)
    ?plants=1      restrict results to Plantae (used by the flower picker)
    """
    q = request.args.get("q", "").strip()
    plants_only = request.args.get("plants", "") == "1"
    if not q:
        return jsonify([])

    params = {"q": q, "per_page": 15}
    if plants_only:
        params["taxon_id"] = PLANTAE_ID
    else:
        params["rank"] = "genus,species,subspecies,variety,family,order,tribe"

    try:
        r = requests.get(f"{INAT_API}/taxa/autocomplete", params=params, timeout=10)
        r.raise_for_status()
        results = []
        for t in r.json().get("results", []):
            photo = t.get("default_photo") or {}
            results.append({
                "id": t.get("id"),
                "name": t.get("name"),
                "rank": t.get("rank"),
                "common_name": t.get("preferred_common_name") or "",
                "thumb": photo.get("square_url") or "",
            })
        return jsonify(results)
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 502


@app.route("/api/observations")
def observations():
    """
    Fetch one page of observations for a taxon.

    Uses id-based ("id_above") pagination rather than page/offset, since
    iNat's API refuses offsets past 10,000 results — id_above scales to
    any dataset size and only requires remembering the last id seen.

    Query params:
      taxon_id  (required) iNat taxon ID to fetch observations for
      per_page  default 200, max 200
      id_above  default 0 — return observations with id greater than this
    """
    taxon_id = request.args.get("taxon_id", type=int)
    per_page = min(request.args.get("per_page", default=200, type=int), 200)
    id_above = request.args.get("id_above", default=0, type=int)

    if not taxon_id:
        return jsonify({"error": "taxon_id required"}), 400

    params = {
        "taxon_id": taxon_id,
        "photos": "true",
        "per_page": per_page,
        "order": "asc",
        "order_by": "id",
        "id_above": id_above,
    }

    try:
        r = requests.get(f"{INAT_API}/observations", params=params, timeout=20)
        if r.status_code == 429:
            return jsonify({
                "error": "rate_limited",
                "retry_after": r.headers.get("Retry-After", "5"),
            }), 429
        r.raise_for_status()
        data = r.json()

        results = []
        for o in data.get("results", []):
            photos = [
                (p.get("url") or "").replace("square", "medium")
                for p in o.get("photos", [])
                if p.get("url")
            ]
            taxon = o.get("taxon") or {}
            results.append({
                "id": o.get("id"),
                "uri": o.get("uri"),
                "observed_on": o.get("observed_on_string") or o.get("observed_on"),
                "place_guess": o.get("place_guess"),
                "quality_grade": o.get("quality_grade"),
                "taxon_name": taxon.get("name"),
                "taxon_common_name": taxon.get("preferred_common_name"),
                "photos": photos,
            })

        last_id = results[-1]["id"] if results else id_above
        return jsonify({
            "results": results,
            "total_results": data.get("total_results", 0),
            "last_id": last_id,
            "has_more": len(results) == per_page,
        })
    except requests.exceptions.RequestException as e:
        return jsonify({"error": str(e)}), 502


@app.route("/")
def index():
    return jsonify({
        "service": "flowerinatminer backend",
        "status": "ok",
        "endpoints": ["/api/taxon/search", "/api/observations", "/health", "/ping"],
    })


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/ping")
def ping():
    return jsonify({"status": "ok", "message": "pong"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7860, debug=False)
