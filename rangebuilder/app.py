"""
Distribution Range Builder – Flask backend
Exact Python logic from the notebook, zero changes to algorithms.
"""

from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import io, json, re, zipfile
import pandas as pd
import geopandas as gpd
import shapely
from shapely.geometry import Polygon, MultiPolygon

app = Flask(__name__, static_folder="static")
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50 MB
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
    # Filter out null-island (0, 0) points
    df = df[~((df["latitude"] == 0) & (df["longitude"] == 0))]
    if df.empty:
        raise ValueError("No valid lat/lon rows after filtering.")
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


# ── DwC / CSV field normalisation ─────────────────────────────────────────────
OUTPUT_COLS = [
    "speciesname", "latitude", "longitude", "recordedby", "datefound",
    "determinedby", "lifestage", "sex", "notes", "rights", "rightsholder",
    "sourcelink", "locality",
]

SPECIES_KEYS = ["species", "speciesname", "scientificname", "taxonname", "verbatimscientificname"]


def normalise_df(df: pd.DataFrame) -> pd.DataFrame:
    """Map raw/DwC column names to our standard schema."""
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    col_lower = {c.lower(): c for c in df.columns}

    def pick(*keys):
        for k in keys:
            if k in col_lower:
                return df[col_lower[k]]
        return pd.Series([None] * len(df), index=df.index)

    out = pd.DataFrame(index=df.index)

    # Species name
    out["speciesname"] = None
    for k in SPECIES_KEYS:
        if k in col_lower:
            out["speciesname"] = df[col_lower[k]]
            break

    # Coordinates
    out["latitude"]  = pick("decimallatitude", "latitude")
    out["longitude"] = pick("decimallongitude", "longitude")

    out["recordedby"]   = pick("recordedby")
    out["determinedby"] = pick("identifiedby", "determinedby")
    out["lifestage"]    = pick("lifestage")
    out["sex"]          = pick("sex")
    out["rightsholder"] = pick("rightsholder")
    out["rights"]       = pick("license", "accessrights", "rights")

    # Date: build from year/month/day if present, else eventDate/datefound
    if all(k in col_lower for k in ("year", "month", "day")):
        yr = df[col_lower["year"]].astype(str).str.strip().replace("nan", "")
        mo = df[col_lower["month"]].astype(str).str.strip().str.zfill(2).replace("nan", "")
        dy = df[col_lower["day"]].astype(str).str.strip().str.zfill(2).replace("nan", "")
        built = (yr + "-" + mo + "-" + dy).where(yr != "" and mo != "" and dy != "", other=None)
        out["datefound"] = built
    else:
        out["datefound"] = pick("eventdate", "datefound")

    # Notes: combine fieldNotes + eventRemarks
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
        out["locality"] = parts.apply(lambda r: ", ".join(v for v in r if v), axis=1).replace("", None)

    # Source link
    out["sourcelink"] = pick("sourcelink", "occurrenceid")

    return out[[c for c in OUTPUT_COLS if c in out.columns]]


# ── File parsing helper ────────────────────────────────────────────────────────
def read_occurrence_file(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """Parse CSV, TSV, TXT, or DwC-A ZIP into a raw DataFrame."""
    fn = filename.lower()
    if fn.endswith(".zip"):
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
            names = zf.namelist()
            occ_name = next((n for n in names if n.lower() == "occurrence.txt"), None)
            if occ_name is None:
                occ_name = next((n for n in names if n.lower().endswith("occurrence.txt")), None)
            if occ_name is None:
                raise ValueError("occurrence.txt not found in ZIP.")
            raw = zf.read(occ_name).decode("utf-8-sig")
        return pd.read_csv(io.StringIO(raw), sep="\t", low_memory=False, dtype=str)
    elif fn.endswith(".csv"):
        text = file_bytes.decode("utf-8-sig")
        return pd.read_csv(io.StringIO(text), sep=",", low_memory=False, dtype=str)
    else:
        # TSV / TXT — auto-detect separator
        text = file_bytes.decode("utf-8-sig")
        first_line = text.split("\n")[0]
        sep = "\t" if "\t" in first_line else ","
        return pd.read_csv(io.StringIO(text), sep=sep, low_memory=False, dtype=str)


# ── JS helpers ─────────────────────────────────────────────────────────────────
def extract_tsv_blocks(js_content):
    blocks = re.findall(r"`(.*?)`", js_content, re.DOTALL)
    return [b.strip() for b in blocks if "latitude" in b]


def tsv_to_df(tsv_text):
    return pd.read_csv(io.StringIO(tsv_text.strip()), sep="\t", low_memory=False)


def to_geojson_str(geom):
    return json.dumps(json.loads(gpd.GeoSeries([geom]).to_json()), separators=(",", ":"))


def build_full_js(tsv_text, geojson_str):
    return f"""// Distribution Range Map
const tsvText = `{tsv_text.strip()}`;

function tsvToGeoJSON(tsv) {{
  const lines = tsv.trim().split(/\\r?\\n/);
  const headers = lines[0].split("\\t").map(h => h.trim());
  const features = [];
  for (let i = 1; i < lines.length; i++) {{
    if (!lines[i].trim()) continue;
    const cols = lines[i].split("\\t");
    const obj = {{}};
    headers.forEach((h, j) => {{ obj[h] = cols[j] ? cols[j].trim() : ""; }});
    const lat = parseFloat(obj["latitude"]);
    const lon = parseFloat(obj["longitude"]);
    if (isNaN(lat) || isNaN(lon) || (lat === 0 && lon === 0)) continue;
    features.push({{
      type: "Feature",
      properties: {{
        name: obj["speciesname"] || "",
        latitude: lat, longitude: lon,
        foundBy: obj["recordedby"] || "",
        dateFound: obj["datefound"] || "",
        determinedBy: obj["determinedby"] || "",
        lifeStage: obj["lifestage"] || "",
        sex: obj["sex"] || "",
        notes: obj["notes"] || "",
        rights: obj["rights"] || "",
        rightsHolder: obj["rightsholder"] || "",
        sourceLink: obj["sourcelink"] || "",
        locality: obj["locality"] || "",
      }},
      geometry: {{ type: "Point", coordinates: [lon, lat] }},
    }});
  }}
  return {{ type: "FeatureCollection", features }};
}}

const speciesData = tsvToGeoJSON(tsvText);
const speciesRangeGeoJSON = {geojson_str};

const map = L.map("map").setView([39, -120], 5);
L.tileLayer("https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png", {{
  maxZoom: 14, attribution: "&copy; OpenStreetMap contributors",
}}).addTo(map);

if (speciesRangeGeoJSON) {{
  L.geoJSON(speciesRangeGeoJSON, {{
    style: {{ color: "#3366ff", weight: 2, fillColor: "#6699ff", fillOpacity: 0.25 }},
    interactive: false,
  }}).addTo(map);
}}

L.geoJSON(speciesData, {{
  pointToLayer: (f, ll) => L.circleMarker(ll, {{
    radius: 5, color: "#ff6600", fillColor: "#ff6600", fillOpacity: 0.9,
  }}).bindPopup(Object.entries(f.properties).map(([k,v]) => `<b>${{k}}:</b> ${{v}}`).join("<br>")),
}}).addTo(map);

if (speciesData.features.length > 0) {{
  const bounds = speciesData.features.map(f => [f.geometry.coordinates[1], f.geometry.coordinates[0]]);
  map.fitBounds(bounds, {{ padding: [10, 10] }});
}}
setTimeout(() => map.invalidateSize(), 700);
"""


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/process", methods=["POST"])
def process():
    """
    Multipart form endpoint.
    input_type=js   → .js file with embedded TSV
    input_type=tsv  → raw tsv_text string in form body
    input_type=file → uploaded CSV / TSV / TXT / DwC-A ZIP
    """
    try:
        input_type = request.form.get("input_type", "tsv")

        if input_type == "js":
            js_file = request.files.get("file")
            if not js_file:
                return jsonify({"error": "No JS file uploaded."}), 400
            js_content = js_file.read().decode("utf-8")
            tsv_blocks = extract_tsv_blocks(js_content)
            if not tsv_blocks:
                return jsonify({"error": "No TSV data found in JS file."}), 400
            df = tsv_to_df(tsv_blocks[0])
            tsv_text = tsv_blocks[0]

        elif input_type == "file":
            uploaded = request.files.get("file")
            if not uploaded:
                return jsonify({"error": "No file uploaded."}), 400
            raw_df = read_occurrence_file(uploaded.read(), uploaded.filename)
            df = normalise_df(raw_df)
            tsv_text = df.fillna("").to_csv(sep="\t", index=False)

        else:  # tsv (used by GBIF API path)
            tsv_text = request.form.get("tsv_text", "").strip()
            if not tsv_text:
                return jsonify({"error": "No TSV data provided."}), 400
            df = tsv_to_df(tsv_text)

        final_range, clean_df = build_range(df)
        geojson_str = to_geojson_str(final_range)
        geojson_dict = json.loads(geojson_str)

        species_name = ""
        for col in ("speciesname", "species", "scientificname"):
            if col in clean_df.columns:
                vals = clean_df[col].dropna()
                if not vals.empty:
                    species_name = str(vals.iloc[0])
                    break

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


@app.route("/process_json", methods=["POST"])
def process_json():
    """
    JSON body endpoint for pre-parsed rows from the GBIF API path.
    Body: { rows: [{speciesname, latitude, longitude, ...}], species_name: "..." }
    """
    try:
        data = request.get_json(force=True)
        rows = data.get("rows", [])
        if not rows:
            return jsonify({"error": "No rows provided."}), 400

        df = pd.DataFrame(rows)
        final_range, clean_df = build_range(df)
        geojson_str = to_geojson_str(final_range)
        geojson_dict = json.loads(geojson_str)

        species_name = data.get("species_name", "")
        if not species_name and "speciesname" in clean_df.columns:
            vals = clean_df["speciesname"].dropna()
            if not vals.empty:
                species_name = str(vals.iloc[0])

        available = [c for c in OUTPUT_COLS if c in df.columns]
        tsv_text = df[available].fillna("").to_csv(sep="\t", index=False)
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