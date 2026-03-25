"""Параметры подключения к PostgreSQL (Airflow + ETL/витрины в Docker)."""

import os
from urllib.parse import quote_plus


def _get_env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def get_sqlalchemy_url() -> str:
    user = _get_env("POSTGRES_USER", "postgres")
    password = _get_env("POSTGRES_PASSWORD", "postgres")
    host = _get_env("POSTGRES_HOST", "postgres")
    port = _get_env("POSTGRES_INTERNAL_PORT", "5432")
    db = _get_env("POSTGRES_DB", "postgres")
    return f"postgresql+psycopg2://{user}:{quote_plus(password)}@{host}:{port}/{db}"


def get_psycopg2_style_url() -> str:
    """Для create_engine с psycopg2 (без +psycopg2 в имени драйвера в URL)."""
    user = _get_env("POSTGRES_USER", "postgres")
    password = _get_env("POSTGRES_PASSWORD", "postgres")
    host = _get_env("POSTGRES_HOST", "postgres")
    port = _get_env("POSTGRES_INTERNAL_PORT", "5432")
    db = _get_env("POSTGRES_DB", "postgres")
    return f"postgresql://{user}:{quote_plus(password)}@{host}:{port}/{db}"


def get_jdbc_url() -> str:
    user = _get_env("POSTGRES_USER", "postgres")
    password = _get_env("POSTGRES_PASSWORD", "postgres")
    host = _get_env("POSTGRES_HOST", "postgres")
    port = _get_env("POSTGRES_INTERNAL_PORT", "5432")
    db = _get_env("POSTGRES_DB", "postgres")
    return f"jdbc:postgresql://{host}:{port}/{db}"


def get_jdbc_properties() -> dict:
    return {
        "user": _get_env("POSTGRES_USER", "postgres"),
        "password": _get_env("POSTGRES_PASSWORD", "postgres"),
        "driver": "org.postgresql.Driver",
    }
