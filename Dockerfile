FROM python:3.11-slim

WORKDIR /app

# Install system deps needed by psycopg2-binary and py-clob-client
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first so this layer is cached until requirements change
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

EXPOSE 8080

# Railway injects PORT env var; app.py reads it with fallback to 8888
CMD ["python", "app.py"]
