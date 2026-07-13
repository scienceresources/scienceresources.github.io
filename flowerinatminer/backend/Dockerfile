FROM python:3.11-slim

RUN useradd -m -u 1000 appuser

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .
COPY index.html .

USER appuser

# Render injects $PORT at runtime; default to 7860 for local/docker-only runs.
ENV PORT=7860
EXPOSE 7860

CMD gunicorn --bind 0.0.0.0:$PORT --timeout 600 --workers 1 --threads 8 --log-level info app:app
