# ── BugGuide Interaction Miner — Flask backend ────────────────────────────────
# Targets HuggingFace Spaces (Docker SDK).
# HF Spaces runs as a non-root user; Playwright Chromium needs a few system deps.
# Port 7861 is the default HF Spaces port.
# ──────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim

# System deps for Playwright Chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    ca-certificates \
    fonts-liberation \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libx11-xcb1 \
    libxcb-dri3-0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    libxshmfence1 \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright's bundled Chromium browser
RUN playwright install chromium

# Copy application
COPY app.py .

# HuggingFace Spaces requires the app to listen on 0.0.0.0:7861
EXPOSE 7860

CMD ["python", "app.py"]