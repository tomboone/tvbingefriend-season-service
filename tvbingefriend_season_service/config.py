"""Configurations for the season service"""
import os
import json
from typing import Any


def _load_local_settings() -> dict[str, Any]:
    """Loads settings from local.settings.json if it exists."""
    try:
        # The config file is in tvbingefriend_show_service/, so go up one level for project root
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        settings_path = os.path.join(project_root, 'local.settings.json')
        with open(settings_path) as f:
            return json.load(f).get("Values", {})
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


_local_settings = _load_local_settings()


def _get_setting(var_name: str, required: bool = True, default: Any = None) -> Any:
    """
    Gets a setting from environment variables or local.settings.json.
    Priority:
    1. Environment variables
    2. local.settings.json
    3. Default value
    """
    value = os.getenv(var_name)
    if value is None:
        value = _local_settings.get(var_name)

    if value is not None:
        return value

    if default is not None:
        return default

    if required:
        raise ValueError(f"Missing required setting: '{var_name}'")
        
    return None


STORAGE_CONNECTION_SETTING_NAME: str = "AzureWebJobsStorage"
STORAGE_CONNECTION_STRING: str | None = _get_setting(STORAGE_CONNECTION_SETTING_NAME, required=False)

SQLALCHEMY_CONNECTION_STRING: str = _get_setting("SQLALCHEMY_CONNECTION_STRING")
MYSQL_SSL_CA_CONTENT: str = _get_setting("MYSQL_SSL_CA_CONTENT", required=False)

SEASONS_QUEUE: str = _get_setting("INDEX_QUEUE", default="index-queue")

SHOW_IDS_TABLE: str = _get_setting("SHOW_IDS_TABLE", default="show_ids_table")

UPDATES_NCRON: str = _get_setting("UPDATES_NCRON", default="0 0 2 * * *")
