# ============================================
# Multi-stage Dockerfile for Django + Tailwind
# Uses uv for Python dependencies
# ============================================

FROM python:3.11-slim AS base

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    g++ \
    default-libmysqlclient-dev \
    pkg-config \
    # ADD FreeTDS Development package here:
    freetds-dev \
    # You might also need the runtime library if it's not pulled in automatically:
    freetds-bin \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js 20.x for Tailwind
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_SYSTEM_PYTHON=1

WORKDIR /app

# ============================================
# Stage 1: Install Python dependencies
# ============================================
FROM base AS python-deps

# Copy dependency files
COPY pyproject.toml ./

# Install Python dependencies using uv
RUN uv pip install --system -r pyproject.toml
RUN uv sync

# ============================================
# Stage 2: Build Tailwind CSS
# ============================================
FROM base AS tailwind-builder

# Copy Python dependencies from previous stage
COPY --from=python-deps /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=python-deps /usr/local/bin /usr/local/bin

# Copy application code
COPY . .

# Install Tailwind dependencies
RUN if [ -f theme/static_src/package.json ]; then \
        cd theme/static_src && npm install; \
    fi

# Build Tailwind CSS for production
RUN python manage.py tailwind build || echo "Tailwind build skipped"

# ============================================
# Stage 3: Final production image
# ============================================
FROM base AS production

# Copy Python dependencies
COPY --from=python-deps /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=python-deps /usr/local/bin /usr/local/bin

# Copy application code
COPY . .

# Copy built Tailwind assets
COPY --from=tailwind-builder /app/theme/static/css /app/theme/static/css

# Create necessary directories
RUN mkdir -p /app/staticfiles /app/mediafiles

# Collect static files
RUN python manage.py collectstatic --noinput || echo "Static collection skipped"

# Expose port
EXPOSE 8000

# Default command (can be overridden in docker-compose)
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
