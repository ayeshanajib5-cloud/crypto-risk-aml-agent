"""Application configuration helpers."""

import os


def get_allowed_origins() -> list[str]:
    """Return CORS origins from ALLOWED_ORIGINS as a comma-separated list."""
    raw_origins = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000,http://localhost:8000")
    origins = [origin.strip() for origin in raw_origins.split(",") if origin.strip()]
    return origins or ["http://localhost:3000"]
