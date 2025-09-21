# ---------- Stage 1: builder ----------
FROM python:3.12-slim AS builder
WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    liblmdb-dev \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --prefix=/install --no-cache-dir -r requirements.txt docker

# Install build helpers
RUN pip install --no-cache-dir setuptools wheel pybind11

COPY . .

# Tell py-lmdb to use system liblmdb instead of patching bundled one
ENV LMDB_FORCE_SYSTEM=1

RUN pip install --prefix=/install ./database/boosts ./py-lmdb


# ---------- Stage 2: runtime ----------
FROM python:3.12-slim
WORKDIR /app

RUN apt-get update && apt-get install -y \
    liblmdb-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local
COPY . .

ENV TYPESENSE_HOST=localhost \
    TYPESENSE_PORT=8108 \
    TYPESENSE_API_KEY=xyz

EXPOSE 8000

CMD ["python", "-m", "sanic", "api.app", "--host=0.0.0.0", "--port=8000"]

