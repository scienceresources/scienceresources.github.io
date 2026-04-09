"""
Distribution Range Builder – Flask backend
"""

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import io, json, re, zipfile, threading
from math import isnan
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests as req_lib
import pandas as pd
import geopandas as gpd
import shapely
from shapely.geometry import Polygon, MultiPolygon

app = Flask(__name__, static_folder="rangebuild")
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50 MB
CORS(app)


# ── iNat / BugGuide exclusion — verbatim from gbif-csv converter ──────────────
EXCLUDED_DATASET_KEYS = {
    '50c9509d-22c7-4a22-a47d-8c48425ef4a7',  # iNaturalist research-grade
    '7f5e4129-0717-428e-876a-464fbd5d9a47',  # BugGuide
}
EXCLUDED_PUBLISHERS = ['inaturalist', 'bugguide']


def exclude_inat(df: pd.DataFrame) -> pd.DataFrame:
    """Filter out iNaturalist and BugGuide rows — matches converter excludeINat logic."""
    col_lower = {c.lower(): c for c in df.columns}
    mask = pd.Series(True, index=df.index)

    if 'datasetkey' in col_lower:
        mask &= ~df[col_lower['datasetkey']].isin(EXCLUDED_DATASET_KEYS)

    for pub_col in ('datasetname', 'institutioncode'):
        if pub_col in col_lower:
            lower_vals = df[col_lower[pub_col]].fillna('').str.lower()
            for ex in EXCLUDED_PUBLISHERS:
                mask &= ~lower_vals.str.contains(ex, na=False)

    excluded = (~mask).sum()
    if excluded:
        print(f"  🚫 Excluded {excluded} iNat/BugGuide rows")
    return df[mask].copy()


# ── Reverse geocoding — verbatim from converter reverseGeocode ────────────────
_geo_cache: dict = {}
_geo_lock = threading.Lock()


def reverse_geocode(lat: float, lon: float) -> str | None:
    """Call bigdatacloud.net — same endpoint and field logic as the converter."""
    key = f"{round(lat, 3)},{round(lon, 3)}"
    with _geo_lock:
        if key in _geo_cache:
            return _geo_cache[key]
    try:
        url = (
            f"https://api.bigdatacloud.net/data/reverse-geocode-client"
            f"?latitude={lat}&longitude={lon}&localityLanguage=en"
        )
        resp = req_lib.get(url, timeout=6)
        d = resp.json()
        parts = [
            d.get("locality") or d.get("city") or "",
            d.get("principalSubdivision") or "",
            d.get("countryName") or "",
        ]
        result = ", ".join(p for p in parts if p) or None
    except Exception:
        result = None
    with _geo_lock:
        _geo_cache[key] = result
    return result


def geocode_missing(df: pd.DataFrame) -> pd.DataFrame:
    """Fill empty locality via reverse geocoding — mirrors converter BATCH=8 logic."""
    df = df.copy()
    empty_mask = df["locality"].isna() | (df["locality"].fillna("").astype(str).str.strip() == "")
    needs_geo = df[empty_mask].dropna(subset=["latitude", "longitude"])

    if needs_geo.empty:
        return df

    print(f"  🌍 Reverse geocoding {len(needs_geo)} records missing locality…")

    def do_geocode(args):
        idx, lat, lon = args
        try:
            la, lo = float(lat), float(lon)
            if not (isnan(la) or isnan(lo)):
                return idx, reverse_geocode(la, lo)
        except (ValueError, TypeError):
            pass
        return idx, None

    tasks = [(row.Index, row.latitude, row.longitude)
             for row in needs_geo.itertuples()]

    BATCH = 8
    with ThreadPoolExecutor(max_workers=BATCH) as ex:
        for idx, result in ex.map(do_geocode, tasks):
            if result:
                df.at[idx, "locality"] = result

    return df

# ── Land mask — lazy-loaded on first use ───────────────────────────────────────
_land_lock = threading.Lock()
_LAND_MASK = None

def get_land_mask():
    global _LAND_MASK
    if _LAND_MASK is not None:
        return _LAND_MASK
    with _land_lock:
        if _LAND_MASK is None:
            print("Downloading Natural Earth 50m land mask…")
            gdf = gpd.read_file("https://naciscdn.org/naturalearth/50m/physical/ne_50m_land.zip")
            _LAND_MASK = gdf.to_crs("EPSG:4326").union_all()
            print("  Land mask ready.")
    return _LAND_MASK


# ── Smoothing ──────────────────────────────────────────────────────────────────
def chaikin_smooth(coords, iterations=6):
    for _ in range(iterations):
        if len(coords) < 3:
            return coords
        new_coords = [coords[0]]
        for i in range(len(coords) - 1):
            p0, p1 = coords[i], coords[i + 1]
            new_coords.append((0.75 * p0[0] + 0.25 * p1[0], 0.75 * p0[1] + 0.25 * p1[1]))
            new_coords.append((0.25 * p0[0] + 0.75 * p1[0], 0.25 * p0[1] + 0.75 * p1[1]))
        new_coords.append(coords[-1])
        coords = new_coords
    return coords


def apply_smoothing(geom, iterations=6):
    if isinstance(geom, Polygon):
        s = chaikin_smooth(list(geom.exterior.coords), iterations=iterations)
        if s[0] != s[-1]:
            s.append(s[0])
        return Polygon(s)
    elif isinstance(geom, MultiPolygon):
        return MultiPolygon([apply_smoothing(p, iterations) for p in geom.geoms])
    return geom


# ── Range builder ──────────────────────────────────────────────────────────────
def build_range(df, buffer_deg=1.2, concave_ratio=0.25, simplify_tol=0.12, smooth_iter=6):
    df = df.copy()
    df["latitude"]  = pd.to_numeric(df["latitude"],  errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude"])
    # Filter null-island (0, 0) points
    df = df[~((df["latitude"] == 0) & (df["longitude"] == 0))]
    if df.empty:
        raise ValueError("No valid coordinate rows after filtering.")
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326"
    )
    all_pts  = gdf.geometry.union_all()
    hull     = shapely.concave_hull(all_pts, ratio=concave_ratio)
    buffered = hull.buffer(buffer_deg, quad_segs=16, join_style=1)
    smoothed = apply_smoothing(buffered.simplify(simplify_tol), iterations=smooth_iter)
    land     = get_land_mask()
    clipped  = smoothed.intersection(land)
    if isinstance(clipped, MultiPolygon):
        valid   = [p for p in clipped.geoms if gdf.geometry.intersects(p).any()]
        clipped = MultiPolygon(valid) if len(valid) > 1 else (valid[0] if valid else clipped)
    return clipped, df


# ── DwC field normalisation ────────────────────────────────────────────────────
OUTPUT_COLS = [
    "speciesname", "latitude", "longitude", "recordedby", "datefound",
    "determinedby", "lifestage", "sex", "notes", "rights", "rightsholder",
    "sourcelink", "gbif_link", "locality",
]

SPECIES_KEYS = ["species", "speciesname", "scientificname", "taxonname", "verbatimscientificname"]


def normalise_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    col_lower = {c.lower(): c for c in df.columns}

    def pick(*keys):
        for k in keys:
            if k in col_lower:
                return df[col_lower[k]]
        return pd.Series([None] * len(df), index=df.index)

    out = pd.DataFrame(index=df.index)

    out["speciesname"] = None
    for k in SPECIES_KEYS:
        if k in col_lower:
            out["speciesname"] = df[col_lower[k]]
            break

    out["latitude"]     = pick("decimallatitude",  "latitude")
    out["longitude"]    = pick("decimallongitude", "longitude")
    out["recordedby"]   = pick("recordedby")
    out["determinedby"] = pick("identifiedby", "determinedby")
    out["lifestage"]    = pick("lifestage")
    out["sex"]          = pick("sex")
    out["rightsholder"] = pick("rightsholder")
    out["rights"]       = pick("license", "accessrights", "rights")

    # Date
    if all(k in col_lower for k in ("year", "month", "day")):
        yr = df[col_lower["year"]].astype(str).str.strip().replace("nan", "")
        mo = df[col_lower["month"]].astype(str).str.strip().str.zfill(2).replace("nan", "")
        dy = df[col_lower["day"]].astype(str).str.strip().str.zfill(2).replace("nan", "")
        out["datefound"] = (yr + "-" + mo + "-" + dy).where(
            (yr != "") & (mo != "") & (dy != ""), other=None
        )
    else:
        out["datefound"] = pick("eventdate", "datefound")

    # Notes
    n1 = pick("fieldnotes", "notes").fillna("").astype(str)
    n2 = pick("eventremarks").fillna("").astype(str)
    combined = (n1 + " " + n2).str.strip()
    out["notes"] = combined.replace("", None)

    # Locality — verbatim from converter buildRow:
    #   parts = [county, stateProvince, countryCode].filter(Boolean).join(', ')
    # The DwC `locality` field is intentionally skipped for DwC-A input because
    # it contains vague strings (e.g. "near Moses Lake"). We only fall back to it
    # for pre-converted TSV files that lack the DwC geo columns entirely.
    dwc_geo = any(k in col_lower for k in ("county", "stateprovince"))
    if dwc_geo:
        parts = pd.concat([
            pick("county").fillna("").astype(str),
            pick("stateprovince").fillna("").astype(str),
            pick("countrycode", "country").fillna("").astype(str),
        ], axis=1)
        built = parts.apply(lambda r: ", ".join(v for v in r if v), axis=1).replace("", None)
        # For rows where all three are blank, fall back to the locality column
        if "locality" in col_lower:
            out["locality"] = built.where(built.notna(), df[col_lower["locality"]])
        else:
            out["locality"] = built
    elif "locality" in col_lower:
        out["locality"] = df[col_lower["locality"]]
    else:
        out["locality"] = None

    # sourcelink: prefer an explicit sourcelink column; if absent, build the
    # GBIF occurrence URL from gbifID (matching the gbif-csv converter output).
    # occurrenceID is intentionally excluded — it's an institution-specific
    # catalog code (e.g. "ORMEL053-25"), not a usable hyperlink.
    if "sourcelink" in col_lower:
        out["sourcelink"] = df[col_lower["sourcelink"]]
    else:
        gbif_id_series = pick("gbifid", "gbif_id")
        out["sourcelink"] = gbif_id_series.apply(
            lambda x: (f"https://www.gbif.org/occurrence/{str(x).strip()}"
                       if pd.notna(x) and str(x).strip() else None)
        )

    # gbif_link: explicit column only (blank in converter output; reserved for
    # manual entry or future use).
    out["gbif_link"] = pick("gbif_link")

    return out[[c for c in OUTPUT_COLS if c in out.columns]]


# ── File parsing ───────────────────────────────────────────────────────────────
def read_occurrence_file(file_bytes: bytes, filename: str) -> pd.DataFrame:
    fn = filename.lower()
    if fn.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
            names    = zf.namelist()
            occ_name = next((n for n in names if n.lower() == "occurrence.txt"), None)
            if occ_name is None:
                occ_name = next((n for n in names if n.lower().endswith("occurrence.txt")), None)
            if occ_name is None:
                raise ValueError("occurrence.txt not found in ZIP.")
            raw = zf.read(occ_name).decode("utf-8-sig")
        return pd.read_csv(io.StringIO(raw), sep="\t", low_memory=False, dtype=str)
    else:
        # occurrence.txt — tab-separated
        text = file_bytes.decode("utf-8-sig")
        return pd.read_csv(io.StringIO(text), sep="\t", low_memory=False, dtype=str)


# ── JS helpers ─────────────────────────────────────────────────────────────────
def to_geojson_str(geom):
    return json.dumps(json.loads(gpd.GeoSeries([geom]).to_json()), separators=(",", ":"))


def build_full_js(tsv_text, geojson_str):
    return f"""// =========================
// Distribution Map Visualization
// =========================

const tsvText = `{tsv_text.strip()}`;

function tsvToGeoJSON(tsv) {{
  const lines = tsv.trim().split(/\\r?\\n/);
  const headers = lines[0].split("\\t").map(h => h.trim());
  const features = [];

  for (let i = 1; i < lines.length; i++) {{
    if (!lines[i].trim()) continue;
    const cols = lines[i].split("\\t");
    if (cols.length < headers.length) continue;

    const obj = {{}};
    headers.forEach((h, j) => {{
      obj[h] = cols[j] ? cols[j].trim() : "";
    }});

    const lat = parseFloat(obj["latitude"]);
    const lon = parseFloat(obj["longitude"]);
    if (isNaN(lat) || isNaN(lon) || (lat === 0 && lon === 0)) continue;

    features.push({{
      type: "Feature",
      properties: {{
        name: obj["speciesname"] || "",
        latitude: lat,
        longitude: lon,
        foundBy: obj["recordedby"] || "",
        dateFound: obj["datefound"] || "",
        determinedBy: obj["determinedby"] || "",
        lifeStage: obj["lifestage"] || "",
        sex: obj["sex"] || "",
        notes: obj["notes"] || "",
        rights: obj["rights"] || "",
        rightsHolder: obj["rightsholder"] || "",
        sourceLink: obj["sourcelink"] || "",
        gbif_link: obj["gbif_link"] || "",
        locality: obj["locality"] || "",
      }},
      geometry: {{ type: "Point", coordinates: [lon, lat] }},
    }});
  }}

  return {{ type: "FeatureCollection", features }};
}}

const melissodesData = tsvToGeoJSON(tsvText);

function isVisible(el) {{
  return el && !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length);
}}

const allPlaceholders = document.querySelectorAll(".map-placeholder");
let target = null;
allPlaceholders.forEach((el) => {{
  if (isVisible(el)) target = el;
}});

const mapEl = document.getElementById("map");
if (target && mapEl) {{
  target.appendChild(mapEl);
  console.log("🗺️ Moved map into:", target.parentElement.className);
}}

// ─── Create map ───────────────────────────────────────────────────────────────
const map = L.map("map").setView([39, -120], 5);
L.tileLayer("https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png", {{
  maxZoom: 14,
  attribution: "&copy; OpenStreetMap contributors",
}}).addTo(map);

// ─── Popup builder ────────────────────────────────────────────────────────────
function buildPopup(p) {{
  const popupParts = [
    `<b>${{p.name}}</b>`,
    `<b>Coordinates:</b> ${{parseFloat(p.latitude).toFixed(4)}}, ${{parseFloat(p.longitude).toFixed(4)}}`,
  ];

  if (p.locality && p.locality.trim())
    popupParts.push(`<b>Locality:</b> ${{p.locality}}`);

  popupParts.push(
    `<b>Recorded By:</b> ${{p.foundBy || "Unknown"}}`,
    `<b>Date Found:</b> ${{p.dateFound || "Unknown"}}`,
    `<b>Determined By:</b> ${{p.determinedBy || "Unknown"}}`,
    `<b>Life Stage:</b> ${{p.lifeStage || "Unknown"}}`,
    `<b>Sex:</b> ${{p.sex || "Unknown"}}`,
    `<b>Notes:</b> ${{p.notes || "None"}}`
  );

  if (p.rights) popupParts.push(`<b>Rights:</b> ${{p.rights}}`);
  if (p.rightsHolder) popupParts.push(`<b>Rights Holder:</b> ${{p.rightsHolder}}`);

  if (p.gbif_link) {{
    popupParts.push(
      `<b>Source:</b> <a href="${{p.gbif_link}}" target="_blank" rel="noopener noreferrer">View GBIF Record</a>`
    );
    if (p.sourceLink) {{
      const link = p.sourceLink.trim();
      if (/^https?:\\/\\//i.test(link))
        popupParts.push(`<b>Discover Life Link:</b> <a href="${{link}}" target="_blank" rel="noopener noreferrer">View Discover Life Record</a>`);
      else
        popupParts.push(`<b>Discover Life Link:</b> ${{link}}`);
    }}
  }} else if (p.sourceLink) {{
    const link = p.sourceLink.trim();
    const isURL = /^https?:\\/\\//i.test(link);
    const isDOI = /^10\\.\\d{{4,9}}\\/[-._;()/:A-Z0-9]+$/i.test(link);
    if (isURL)
      popupParts.push(`<b>Source:</b> <a href="${{link}}" target="_blank" rel="noopener noreferrer">View Record</a>`);
    else if (isDOI)
      popupParts.push(`<b>Source:</b> <a href="https://doi.org/${{link}}" target="_blank" rel="noopener noreferrer">${{link}}</a>`);
    else
      popupParts.push(`<b>Source:</b> ${{link}}`);
  }}

  return popupParts.join("<br>");
}}

// ─── Zoom-aware point layer ───────────────────────────────────────────────────
function zoomToRadius(zoom) {{
  if (zoom >= 12) return 7;
  if (zoom >= 10) return 6;
  if (zoom >= 8)  return 5;
  if (zoom >= 6)  return 4;
  if (zoom >= 5)  return 3;
  return 2;
}}

// ─── Age-based color helpers ──────────────────────────────────────────────────
function parseObservationYear(dateFound) {{
  if (!dateFound || !dateFound.trim()) return null;
  const match = dateFound.trim().match(/(\d{{4}})/);
  return match ? parseInt(match[1], 10) : null;
}}

function darkenHex(hex, factor) {{
  const f = Math.max(0, Math.min(1, factor));
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  return "#" +
    Math.round(r * (1 - f)).toString(16).padStart(2, "0") +
    Math.round(g * (1 - f)).toString(16).padStart(2, "0") +
    Math.round(b * (1 - f)).toString(16).padStart(2, "0");
}}

// Returns full Leaflet circleMarker options based on observation age.
// Unknown dates get a dashed stroke so they are visually distinct from any age band.
function ageMarkerOptions(dateFound, baseColor, radius) {{
  const year = parseObservationYear(dateFound);
  if (year === null) {{
    const c = darkenHex(baseColor, 0.55);
    return {{ radius, color: c, fillColor: c, fillOpacity: 0.35, weight: 2, dashArray: "5,4" }};
  }}
  const age = new Date().getFullYear() - year;
  const period = Math.floor(age / 20);
  const c = darkenHex(baseColor, Math.min(period * 0.20, 0.80));
  return {{ radius, color: c, fillColor: c, fillOpacity: 0.9, weight: 1 }};
}}

function addPointLayer(geojson, baseColor) {{
  const layer = L.geoJSON(geojson, {{
    pointToLayer: (feature, latlng) => {{
      const opts = ageMarkerOptions(feature.properties.dateFound, baseColor, zoomToRadius(map.getZoom()));
      const marker = L.circleMarker(latlng, opts);
      marker.bindPopup(buildPopup(feature.properties));
      return marker;
    }},
  }}).addTo(map);

  map.on("zoomend", () => {{
    const r = zoomToRadius(map.getZoom());
    layer.eachLayer(m => m.setRadius(r));
  }});

  return layer;
}}

// ─── Age legend ───────────────────────────────────────────────────────────────
function addAgeLegend(baseColor) {{
  const legend = L.control({{ position: "bottomright" }});
  legend.onAdd = function() {{
    const div = L.DomUtil.create("div");
    div.style.cssText = "background:white;padding:10px 14px;border-radius:10px;box-shadow:0 2px 8px rgba(0,0,0,0.15);font-family:sans-serif;font-size:12px;line-height:1.7;min-width:170px;";
    const entries = [
      {{ label: "Last 20 years",   factor: 0.00 }},
      {{ label: "20–39 years ago", factor: 0.20 }},
      {{ label: "40–59 years ago", factor: 0.40 }},
      {{ label: "60–79 years ago", factor: 0.60 }},
      {{ label: "80+ years ago",   factor: 0.80 }},
      { label: "Date unknown",    factor: 0.55, dashed: true },
    ];
    div.innerHTML = `<div style="font-weight:700;font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:#475569;margin-bottom:6px;">Observation Age</div>` +
      entries.map(e => {{
        const c = darkenHex(baseColor, e.factor);
        const border = e.dashed ? `border:2px dashed ${{c}};background:transparent;` : `background:${{c}};`;
        return `<div style="display:flex;align-items:center;gap:8px;">
          <span style="display:inline-block;width:12px;height:12px;border-radius:50%;${{border}}flex-shrink:0;"></span>
          <span style="color:#475569;">${{e.label}}</span>
        </div>`;
      }}).join("") ;
    return div;
  }};
  legend.addTo(map);
}}

// ─── PASTE GEOJSON HERE (AUTOMATED) ──────────────────────────────────────────
const speciesRangeGeoJSON = {geojson_str};

// ─── Draw range polygon (beneath points) ─────────────────────────────────────
if (speciesRangeGeoJSON) {{
  L.geoJSON(speciesRangeGeoJSON, {{
    style: {{
      color: "#3366ff",
      weight: 2,
      fillColor: "#6699ff",
      fillOpacity: 0.25,
      smoothFactor: 1,
    }},
    interactive: false,
  }}).addTo(map);
}}

// ─── Draw points (on top of polygon) ─────────────────────────────────────────
const pointLayer = addPointLayer(melissodesData, "#ff6600");
addAgeLegend("#ff6600");

// ─── Fit bounds to all valid points ──────────────────────────────────────────
if (melissodesData.features.length > 0) {{
  const bounds = melissodesData.features.map(f => [
    f.geometry.coordinates[1],
    f.geometry.coordinates[0],
  ]);
  map.fitBounds(bounds, {{ padding: [10, 10] }});
}}

// Resize handlers
window.addEventListener("resize", () => {{
  setTimeout(() => map.invalidateSize(), 500);
}});
setTimeout(() => map.invalidateSize(), 700);

window.addEventListener("resize", () => {{
  setTimeout(() => {{
    const allPlaceholders = document.querySelectorAll(".map-placeholder");
    const mapEl = document.getElementById("map");
    let target = null;
    allPlaceholders.forEach((el) => {{
      if (el.offsetParent !== null) target = el;
    }});
    if (target && mapEl && mapEl.parentElement !== target) {{
       target.appendChild(mapEl);
       map.invalidateSize();
    }}
  }}, 600);
}});
"""


# ── Routes ─────────────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return send_from_directory("rangebuild", "index.html")


@app.route("/process", methods=["POST"])
def process():
    """
    input_type=file  → uploaded occurrence.txt or DwC-A ZIP
    input_type=tsv   → raw tsv_text string (used by GBIF API path in frontend)
    """
    try:
        input_type = request.form.get("input_type", "file")

        if input_type == "file":
            uploaded = request.files.get("file")
            if not uploaded:
                return jsonify({"error": "No file uploaded."}), 400
            raw_df = read_occurrence_file(uploaded.read(), uploaded.filename)
            raw_df = exclude_inat(raw_df)        # filter iNat/BugGuide (converter logic)
            df     = normalise_df(raw_df)
            df     = geocode_missing(df)         # reverse-geocode blank localities
        else:
            tsv_text = request.form.get("tsv_text", "").strip()
            if not tsv_text:
                return jsonify({"error": "No TSV data provided."}), 400
            df = pd.read_csv(io.StringIO(tsv_text), sep="\t", low_memory=False)
            df = geocode_missing(df)             # also geocode API/TSV path

        final_range, clean_df = build_range(df)
        geojson_str  = to_geojson_str(final_range)
        geojson_dict = json.loads(geojson_str)

        species_name = ""
        for col in ("speciesname", "species", "scientificname"):
            if col in clean_df.columns:
                vals = clean_df[col].dropna()
                if not vals.empty:
                    species_name = str(vals.iloc[0])
                    break

        available = [c for c in OUTPUT_COLS if c in df.columns]
        tsv_out   = df[available].fillna("").to_csv(sep="\t", index=False)
        js_output = build_full_js(tsv_out, geojson_str)

        return jsonify({
            "geojson":      geojson_dict,
            "geojson_str":  geojson_str,
            "js_output":    js_output,
            "record_count": len(clean_df),
            "species_name": species_name,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5000)