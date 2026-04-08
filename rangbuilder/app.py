"""
Distribution Range Builder – Flask backend
Exact Python logic from the notebook, zero changes to algorithms.
"""

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import io, json, re
import pandas as pd
import geopandas as gpd
import shapely
from shapely.geometry import Polygon, MultiPolygon

app = Flask(__name__, static_folder="static")
CORS(app)

# ── Land mask (loaded once at startup) ─────────────────────────────────────────
print("Downloading Natural Earth 50m land mask...")
_land_gdf = gpd.read_file("https://naciscdn.org/naturalearth/50m/physical/ne_50m_land.zip")
LAND_MASK = _land_gdf.to_crs("EPSG:4326").union_all()
print("  Land mask ready.")


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
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df = df.dropna(subset=["latitude", "longitude"])
    if df.empty:
        raise ValueError("No valid lat/lon rows.")
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="EPSG:4326"
    )
    all_pts = gdf.geometry.union_all()
    hull = shapely.concave_hull(all_pts, ratio=concave_ratio)
    buffered = hull.buffer(buffer_deg, quad_segs=16, join_style=1)
    smoothed = apply_smoothing(buffered.simplify(simplify_tol), iterations=smooth_iter)
    clipped = smoothed.intersection(LAND_MASK)
    if isinstance(clipped, MultiPolygon):
        valid = [p for p in clipped.geoms if gdf.geometry.intersects(p).any()]
        clipped = MultiPolygon(valid) if len(valid) > 1 else (valid[0] if valid else clipped)
    return clipped, df


# ── JS helpers ─────────────────────────────────────────────────────────────────
def extract_tsv_blocks(js_content):
    """Return a list of TSV strings found inside backtick template literals."""
    blocks = re.findall(r"`(.*?)`", js_content, re.DOTALL)
    return [b.strip() for b in blocks if "latitude" in b]


def tsv_to_df(tsv_text):
    return pd.read_csv(io.StringIO(tsv_text.strip()), sep="\t", low_memory=False)


def to_geojson_str(geom):
    """Compact JSON string of the geometry as a FeatureCollection."""
    return json.dumps(json.loads(gpd.GeoSeries([geom]).to_json()), separators=(",", ":"))


def inject_geojson(js_content, geojson_str):
    pattern = r"const speciesRangeGeoJSON\s*=\s*null;"
    replacement = f"const speciesRangeGeoJSON = {geojson_str};"
    updated, n = re.subn(pattern, replacement, js_content)
    if n == 0:
        raise ValueError('Could not find "const speciesRangeGeoJSON = null;" in the JS file.')
    return updated


def build_full_js(tsv_text, geojson_str):
    """Generate the full .js output from TSV + polygon GeoJSON string."""
    return f"""// =========================
// Melissodes Map Visualization
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
    if (isNaN(lat) || isNaN(lon)) continue;

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
    const isDOI = /^10\\.\\d{{4,9}}\\/[-._;()\\/:A-Z0-9]+$/i.test(link);
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
// ─────────────────────────────────────────────────────────────────────────────

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
    if (target && mapEl && target !== mapEl.parentElement) {{
      target.appendChild(mapEl);
      if (typeof map !== "undefined" && map.invalidateSize) map.invalidateSize();
    }}
  }}, 600);
}});
"""


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/process", methods=["POST"])
def process():
    """
    Accepts multipart form with:
      - mode: 'species' or 'subspecies'
      - file: .js file  (optional, if js_mode)
      - tsv_text: raw TSV string  (optional, if tsv_mode)
      - input_type: 'js' | 'tsv'
    Returns JSON: { geojson, js_output, record_count, species_name }
    """
    try:
        mode = request.form.get("mode", "species")
        input_type = request.form.get("input_type", "tsv")

        if input_type == "js":
            js_file = request.files.get("file")
            if not js_file:
                return jsonify({"error": "No JS file uploaded."}), 400
            js_content = js_file.read().decode("utf-8")
            tsv_blocks = extract_tsv_blocks(js_content)
            if not tsv_blocks:
                return jsonify({"error": "No TSV data found inside backtick blocks in the JS file."}), 400
            tsv_text = tsv_blocks[0]
        else:
            tsv_text = request.form.get("tsv_text", "").strip()
            if not tsv_text:
                return jsonify({"error": "No TSV data provided."}), 400

        df = tsv_to_df(tsv_text)
        final_range, clean_df = build_range(df)
        geojson_str = to_geojson_str(final_range)
        geojson_dict = json.loads(geojson_str)

        # Derive species name
        species_name = ""
        if "speciesname" in clean_df.columns and not clean_df["speciesname"].dropna().empty:
            species_name = clean_df["speciesname"].dropna().iloc[0]

        # Build JS output
        js_output = build_full_js(tsv_text, geojson_str)

        return jsonify({
            "geojson": geojson_dict,
            "geojson_str": geojson_str,
            "js_output": js_output,
            "record_count": len(clean_df),
            "species_name": species_name,
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5000)
