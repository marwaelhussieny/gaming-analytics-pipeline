"""
Unit tests for the Steam ingestion module.

These don't hit the real API (no network calls in unit tests) — they test
the retry/failure-handling logic in isolation using mocked responses.
"""

from unittest.mock import patch, MagicMock

import pytest
import requests

from src.ingestion.steam_api import fetch_player_count


@patch("src.ingestion.steam_api.requests.get")
def test_fetch_player_count_success(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "response": {"result": 1, "player_count": 500000}
    }
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    result = fetch_player_count(570)

    assert result["app_id"] == 570
    assert result["player_count"] == 500000
    assert "fetched_at" in result


@patch("src.ingestion.steam_api.requests.get")
@patch("src.ingestion.steam_api.time.sleep", return_value=None)  # skip real backoff delay in tests
def test_fetch_player_count_retries_then_fails(mock_sleep, mock_get):
    mock_get.side_effect = requests.exceptions.ConnectionError("connection error")

    result = fetch_player_count(730)

    assert result["player_count"] is None
    assert "error" in result
    assert mock_get.call_count == 3  # MAX_RETRIES


@patch("src.ingestion.steam_api.requests.get")
def test_fetch_player_count_handles_api_error_result(mock_get):
    mock_response = MagicMock()
    mock_response.json.return_value = {"response": {"result": 42}}  # non-success code
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    result = fetch_player_count(9999)

    assert result["player_count"] is None
