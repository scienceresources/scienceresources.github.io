FROM python:3.11-slim

RUN useradd -m -u 1000 appuser

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 7860

USER appuser

CMD ["gunicorn", \
     "--bind", "0.0.0.0:7860", \
     "--timeout", "600", \
     "--workers", "1", \
     "--threads", "8", \
     "--log-level", "info", \
     "app:app"]