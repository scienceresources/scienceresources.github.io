"""
Distribution Range Builder – Flask backend
"""

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import io, json, re, zipfile, threading
import pandas as pd
import geopandas as gpd
import shapely
from shapely.geometry import Polygon, MultiPolygon

app = Flask(__name__, static_folder="rangebuild")
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50 MB
CORS(app)

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

    # Locality
    if "locality" in col_lower:
        out["locality"] = df[col_lower["locality"]]
    else:
        parts = pd.concat([
            pick("county").fillna("").astype(str),
            pick("stateprovince").fillna("").astype(str),
            pick("countrycode", "country").fillna("").astype(str),
        ], axis=1)
        out["locality"] = parts.apply(
            lambda r: ", ".join(v for v in r if v), axis=1
        ).replace("", None)

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

function addPointLayer(geojson, color) {{
  const layer = L.geoJSON(geojson, {{
    pointToLayer: (feature, latlng) => {{
      const marker = L.circleMarker(latlng, {{
        radius: zoomToRadius(map.getZoom()),
        color: color,
        fillColor: color,
        fillOpacity: 0.9,
      }});
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
            df     = normalise_df(raw_df)
        else:
            tsv_text = request.form.get("tsv_text", "").strip()
            if not tsv_text:
                return jsonify({"error": "No TSV data provided."}), 400
            df = pd.read_csv(io.StringIO(tsv_text), sep="\t", low_memory=False)

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