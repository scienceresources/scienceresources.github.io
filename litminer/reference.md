# Melissodes Literature Miner — Project Reference

## What This Is

A Flask web app (HuggingFace Spaces / GitHub Pages-ready) that mines five academic/archival sources
(OpenAlex, Crossref, Semantic Scholar, PubMed, BHL) for literature on
**Melissodes bees and their predators / parasites / kleptoparasites**.

The user supplies:
- Their email (required — OpenAlex polite pool)
- API keys: Semantic Scholar, PubMed/NCBI, BHL (all optional but improve rate limits)
- Optionally: an OpenRouter key for AI taxon classification (free model available)
- A newline or comma-separated taxa list (predator/parasite genera)

---

## File Structure

```
melissodes_miner/
├── app.py                          ← Flask backend (all logic)
├── requirements.txt                ← pip deps for HuggingFace
├── Dockerfile                      ← HuggingFace Docker Space config
├── static/
│   └── index.html                  ← Single-page frontend
├── .github/
│   └── workflows/
│       └── keepalive.yml           ← GitHub Actions pinger (prevents HF sleep)
└── reference.md                    ← This file
```

---

## HuggingFace Spaces Deployment

### Step-by-step

1. Go to https://huggingface.co/new-space
2. Name your Space (e.g. `melissodes-miner`)
3. Select **Docker** as the SDK
4. Set visibility to **Public** (or Private if you prefer)
5. Clone the Space repo locally:
   ```bash
   git clone https://huggingface.co/spaces/YOUR_USERNAME/melissodes-miner
   cd melissodes-miner
   ```
6. Copy in your files:
   ```
   app.py
   requirements.txt
   Dockerfile
   static/index.html
   ```
7. Commit and push:
   ```bash
   git add .
   git commit -m "Initial deploy"
   git push
   ```
8. HuggingFace will build and launch automatically. Watch the **Logs** tab in the Space UI.
9. Your app will be live at: `https://YOUR_USERNAME-melissodes-miner.hf.space`

### Notes
- Use `--timeout 600` (already in Dockerfile) so long OCR jobs don't time out
- Use `--workers 1 --threads 8` — the job queue is thread-based, not process-based
- The `/ping` route returns `OK 200` — this is the keepalive target

---

## Preventing HuggingFace Sleep (Uptime Pinger)

Free HF Spaces go to sleep after ~15 minutes of inactivity. Two options:

### Option A — GitHub Actions (recommended, free)

The repo includes `.github/workflows/keepalive.yml` which pings your Space every 5 minutes.

Setup:
1. Push your code to a GitHub repo (separate from the HF Space repo, or use the same via a mirror)
2. In your GitHub repo → **Settings → Secrets and variables → Actions**, add:
   - `HF_SPACE_SUBDOMAIN` = `YOUR_USERNAME-melissodes-miner`
     (i.e. the subdomain portion of `https://YOUR_USERNAME-melissodes-miner.hf.space`)
3. Go to **Actions** tab → enable workflows
4. The pinger runs automatically every 5 minutes on GitHub's servers for free

### Option B — UptimeRobot (external, also free)

1. Create a free account at https://uptimerobot.com
2. Add a new monitor: **HTTP(s)** type
3. URL: `https://YOUR_USERNAME-melissodes-miner.hf.space/ping`
4. Monitoring interval: **5 minutes**
5. That's it — UptimeRobot will ping the Space and keep it awake

---

## Running on GitHub (Frontend-only mode)

`static/index.html` is a self-contained single-page app. If you want to host just the frontend on
GitHub Pages (e.g. as a demo/landing page that points users to the HF Space):

1. In your GitHub repo settings → **Pages** → set source to `main` branch, `/static` folder
   (or copy `index.html` to the repo root and set source to root)
2. The frontend will load, but all API calls (`/start`, `/stream`, etc.) need to hit the HF Space.
3. Update the `fetch` base URL in `index.html` if you want the GitHub Pages version to proxy to HF:
   ```js
   // Near the top of the Alpine data block, add:
   apiBase: 'https://YOUR_USERNAME-melissodes-miner.hf.space',
   ```
   Then prefix every `fetch('/start', ...)` call with `this.apiBase`.

For the **full app** (backend + frontend together), deploy to HuggingFace — GitHub Pages is
static-only and can't run Flask.

---

## Architecture

### Backend (`app.py`)

| Component | Description |
|-----------|-------------|
| `JOBS` dict | In-memory job store keyed by UUID |
| `JOB_QUEUE` | Thread-safe FIFO queue — one job runs at a time |
| `_QUEUE_THREAD` | Single background daemon thread that drains `JOB_QUEUE` |
| `ACTIVE_JOB` | Tracks which job_id is currently executing |
| `POST /start` | Validates input, enqueues job, returns `job_id` + `queue_position` |
| `POST /resume` | Loads `.litminer` checkpoint, enqueues resume job |
| `GET /stream/<job_id>` | SSE endpoint — streams log queue until job ends |
| `GET /status/<job_id>` | Polling fallback for SSE-unfriendly clients |
| `GET /download/csv/<job_id>` | Returns completed CSV as file download (any time) |
| `GET /download/checkpoint/<job_id>` | Returns `.litminer` ZIP at any point mid-run |
| `GET /queue` | Returns `{ active_job, queue_depth }` |
| `POST /api/ai/classify` | OpenRouter AI taxon classifier (free model default) |
| `GET /ping` | Keepalive endpoint → returns `OK 200` |

### Job Queue / Concurrency

Only one job runs at a time. When a second user submits a job while one is active:
- Their job is placed in `JOB_QUEUE` (Python `queue.Queue` — thread-safe FIFO)
- `/start` returns `queue_position > 0`
- The frontend shows a "Queued" banner and keeps the SSE stream open
- When the active job finishes, the queue worker dequeues the next job automatically
- The queued user's SSE stream starts receiving log messages at that point

This prevents resource contention on the single HF Space instance. For a production
multi-user deployment, use a task queue (Celery + Redis) instead.

### Job Thread (`run_job`)

Runs five phases in sequence:

| Phase | Description |
|-------|-------------|
| Phase 1 — Literature Search | Standard + predator-centric queries across all enabled APIs |
| Phase 2 — Full-text scan | Fetches open-access full text for records without useful abstracts |
| Phase 3 — BHL Search | Queries Biodiversity Heritage Library API |
| Phase 4 — BHL OCR | Fetches OCR text for BHL records via Internet Archive |
| Phase 5 — Export | Builds pandas DataFrame → CSV stored in job dict |

At the end of each phase, a **milestone** log entry is emitted (level `"milestone"`).
The SSE payload includes `is_milestone: true` which the frontend uses to:
- Pulse/highlight the `.litminer` download button
- Fire a browser notification (if the user has enabled them)

### Checkpoint System

- **Download anytime**: `GET /download/checkpoint/<job_id>` works mid-run — it serializes the
  current in-memory state to a `.litminer` ZIP without pausing or interrupting the job
- **In-memory**: checkpoints are RAM-only during a run; the `.litminer` file is the persistence layer
- **Resume**: upload a `.litminer` at the Resume section; the job picks up from the saved phase

---

## AI Taxon Classification (OpenRouter)

### Why it exists

The original "prey bees" query generation assumed all taxa preyed on bees, which produced wrong
search terms for e.g. bee flies (which parasitize, not prey on, bees), kleptoparasites, or
taxa that primarily attack other insects. The AI step asks the model:

> *"What is [Taxon]? Is it a predator, parasite, or kleptoparasite?"*

and returns precise `prey_order_term`, `prey_common_term`, `host_term` fields that replace the
GBIF-derived defaults.

### Default model

`openai/gpt-4o-mini:free` — free tier on OpenRouter, no cost to users.
Users can switch to paid models (GPT-4o, Gemini Pro, Claude Sonnet) for higher accuracy on
obscure taxa.

### Endpoint

`POST /api/ai/classify`
```json
{ "taxon_name": "Philanthus", "openrouter_key": "sk-or-v1-...", "model": "openai/gpt-4o-mini:free" }
```
Returns:
```json
{
  "animal_type": "bee wolf wasp",
  "is_predator": true, "is_parasite": false, "is_kleptoparasite": false,
  "prey_order_term": "prey Hymenoptera",
  "prey_common_term": "prey bees",
  "host_term": "",
  "rationale": "Philanthus wasps are solitary predatory wasps that provision nests with paralyzed bees.",
  "notes": ""
}
```

---

## API Source Notes

| Source | Auth | Rate limit (no key) | Rate limit (with key) |
|--------|------|--------------------|-----------------------|
| OpenAlex | email (polite pool) | ~10 req/s | same |
| Crossref | email (polite pool) | ~50 req/s | same |
| Semantic Scholar | `x-api-key` header | 1 req/s | 10 req/s |
| PubMed | `api_key` param | 3 req/s | 10 req/s |
| BHL | `apikey` param | limited | higher with key |

All keys are user-supplied at runtime — never hardcoded.

---

## Frontend (`static/index.html`)

Single-page app, no build step. Uses Tailwind CDN, Font Awesome, Alpine.js.

### UI sections

1. **Target Taxon** — GBIF autocomplete search
2. **API Credentials** — email (required), S2 key, PubMed key, BHL key
3. **Literature Sources** — toggle each of the 5 sources
4. **Predator / Parasite Taxa** — paste or load sample taxa list
5. **AI Taxon Classification** — OpenRouter key + model picker + "Classify" button
6. **Resume from Checkpoint** — collapsible; upload `.litminer` to resume
7. **Notification toggle** — enable browser notifications for milestone checkpoints

### Run tab features

- Live SSE log terminal with colour-coded levels (`info`, `warn`, `error`, `checkpoint`, `milestone`)
- Progress bars per phase
- Stats bar (hits, check, noise, API calls)
- **Queue banner** — shown when job is waiting behind another user's job
- **Always-available `.litminer` download button** — pulses amber when a milestone fires
- CSV download (available when done or stopped)

---

## What To Improve Next

- [ ] Add per-job result isolation so multiple users run truly concurrently (needs Celery + Redis)
- [ ] Add email notification when job finishes (for very long runs)
- [ ] Allow taxa `.txt` file upload instead of pasting
- [ ] Add a progress bar percentage overlay on phase badges
- [ ] Expose `RESULTS_PER_QUERY` and `FETCH_FULLTEXT` as UI toggles
- [ ] Persist jobs to disk so server restarts don't lose in-flight work
- [ ] Add a "Share results" link (upload CSV to HF dataset hub)
