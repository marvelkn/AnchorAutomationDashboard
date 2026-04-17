"""WSGI entry point for production deployment (gunicorn wsgi:server)."""
from app import server  # noqa: F401
