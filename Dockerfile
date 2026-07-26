# Smart Scanner Backend Dockerfile
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# --- Deployment build provenance (provider-neutral) ------------------------- #
# The source revision is EMBEDDED at build time via build args and promoted to
# runtime env vars, so the running container can prove its revision without
# ever running git at runtime. Never hardcode a SHA here — pass it at build:
#   docker build --build-arg APP_GIT_SHA="$(git rev-parse HEAD)" ...
ARG APP_GIT_SHA=unknown
ARG APP_BUILD_TIME=unknown
ARG APP_RELEASE=unknown
ARG APP_ENVIRONMENT=production

ENV APP_GIT_SHA=${APP_GIT_SHA} \
    APP_BUILD_TIME=${APP_BUILD_TIME} \
    APP_RELEASE=${APP_RELEASE} \
    APP_ENVIRONMENT=${APP_ENVIRONMENT}

# Standard OCI image labels (safe, non-sensitive metadata only).
LABEL org.opencontainers.image.title="smart-scanner-be" \
      org.opencontainers.image.version="1.1.0" \
      org.opencontainers.image.revision="${APP_GIT_SHA}" \
      org.opencontainers.image.created="${APP_BUILD_TIME}"

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash scanner && \
    chown -R scanner:scanner /app
USER scanner

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
