FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (Docker layer caching —
# if requirements.txt hasn't changed, this layer is reused)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY websocket_producer.py .

# Run as non-root — security best practice for financial systems
RUN adduser --disabled-password --gecos "" appuser
USER appuser

CMD ["python", "websocket_producer.py"]
