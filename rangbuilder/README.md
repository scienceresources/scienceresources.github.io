# 🐝 Distribution Range Builder — Web App

A web interface for the **Distribution Range Builder** notebook. Upload a `.js` file or paste TSV data, and the Python backend runs the exact same pipeline as the original notebook:

- **Concave hull** (Shapely)
- **Chaikin smoothing** (6 iterations)
- **Buffer + simplify** (1.2°, tol 0.12)
- **Land-clip** via Natural Earth 50m mask

Outputs a `.geojson` range file **and** a ready-to-use `.js` file with the GeoJSON baked in.

---

## Quick Start (local)

```bash
pip install -r requirements.txt
python app.py
```

Then open **http://localhost:5000** in your browser.

---

## Deploy to GitHub Pages + a Python host

The app is split into two parts:

### 1. Frontend (GitHub Pages)
The `static/index.html` is a fully self-contained page. You can host it on GitHub Pages for free:

1. Push this repo to GitHub
2. Go to **Settings → Pages → Source**: set to `main` branch, `/static` folder (or `/docs` if you rename it)
3. Update the `API` constant at the top of the `<script>` block in `index.html` to point at your deployed backend URL:
   ```js
   const API = 'https://your-backend.onrender.com';  // ← update this
   ```

### 2. Backend (Python host)

Any WSGI-compatible host works. Easiest free options:

#### Render.com (recommended — free tier)
1. Create a **Web Service** pointing at this repo
2. Build command: `pip install -r requirements.txt`
3. Start command: `gunicorn app:app`
4. Set env var: `PYTHON_VERSION=3.11`

#### Railway.app
1. New project → Deploy from GitHub repo
2. Railway auto-detects Flask; it'll work out of the box.

#### Fly.io
```bash
fly launch
fly deploy
```

---

## File structure

```
range-builder/
├── app.py              ← Flask backend (all Python logic)
├── requirements.txt
├── static/
│   └── index.html      ← Frontend (single file, no build step)
└── README.md
```

---

## Algorithm parameters (unchanged from notebook)

| Parameter | Value | Effect |
|-----------|-------|--------|
| `buffer_deg` | 1.2° | Expands hull outward |
| `concave_ratio` | 0.25 | Controls concave hull tightness |
| `simplify_tol` | 0.12 | Removes micro-detail before smoothing |
| `smooth_iter` | 6 | Chaikin smoothing passes |
| `quad_segs` | 16 | Buffer curve resolution |

These are hardcoded identically to the notebook and **must not be changed** — they define the visual style of the range polygons.
