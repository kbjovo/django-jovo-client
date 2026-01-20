# ============================================
# Multi-stage Dockerfile for Django + Tailwind
# Uses uv + uv.lock
# Compatible with uv versions without --system
# ============================================

FROM python:3.13-slim AS base

# -----------------------------
# System dependencies
# -----------------------------
RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    g++ \
    pkg-config \
    default-libmysqlclient-dev \
    freetds-dev \
    freetds-bin \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------
# Node.js 20.x for Tailwind
# -----------------------------
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------
# Install uv
# -----------------------------
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# -----------------------------
# Env
# -----------------------------
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

WORKDIR /app

# ============================================
# Stage 1: Install Python dependencies (locked)
# ============================================
FROM base AS python-deps

COPY pyproject.toml uv.lock ./

# Create venv + install deps from lock
RUN uv sync --frozen --no-dev

# ============================================
# Stage 2: Build Tailwind CSS
# ============================================
FROM base AS tailwind-builder

# Copy the venv from deps stage
COPY --from=python-deps /opt/venv /opt/venv

COPY . .

RUN if [ -f theme/static_src/package.json ]; then \
        cd theme/static_src && npm ci; \
    fi

RUN python manage.py tailwind build || echo "Tailwind build skipped"

# ============================================
# Stage 3: Final production image
# ============================================
FROM base AS production

# Copy venv
COPY --from=python-deps /opt/venv /opt/venv

# Copy app code
COPY . .

# Copy built Tailwind output
COPY --from=tailwind-builder /app/theme/static/css /app/theme/static/css

RUN mkdir -p /app/staticfiles /app/mediafiles

RUN python manage.py collectstatic --noinput || echo "Static collection skipped"

EXPOSE 8000

CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
