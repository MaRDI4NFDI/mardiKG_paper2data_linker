import io
import json
import sys
import urllib.error
from pathlib import Path
from typing import List

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from tasks import ucimlrepo_get_datasets as get_datasets_module


class _FakeLogger:
    def __init__(self) -> None:
        self.messages: List[str] = []

    def info(self, message: str, *args, **kwargs) -> None:
        formatted = message % args if args else message
        self.messages.append(formatted)


def test_get_available_datasets_success(monkeypatch):
    """Return parsed dataset list and hit the expected API URL when request succeeds."""
    fake_logger = _FakeLogger()
    monkeypatch.setattr(get_datasets_module, "get_logger_safe", lambda: fake_logger)

    call_args = {}

    def fake_urlopen(url, *args, **kwargs):
        call_args["url"] = url
        return io.BytesIO(
            json.dumps(
                {
                    "status": 200,
                    "data": [
                        {"id": 1, "name": "Dataset 1"},
                        {"id": 2, "name": "Dataset 2"},
                    ],
                }
            ).encode("utf-8")
        )

    monkeypatch.setattr(get_datasets_module.urllib.request, "urlopen", fake_urlopen)

    datasets = get_datasets_module.get_available_datasets.fn(
        filter="Python", search="Iris", area="Education"
    )

    expected_url = (
        "https://archive.ics.uci.edu/api/datasets/list"
        "?filter=python&search=iris&area=Education"
    )
    assert call_args["url"] == expected_url
    assert datasets == [
        {"id": 1, "name": "Dataset 1"},
        {"id": 2, "name": "Dataset 2"},
    ]


def test_get_available_datasets_handles_non_200_status(monkeypatch):
    """Raise ValueError with API error message when response status != 200."""
    monkeypatch.setattr(get_datasets_module, "get_logger_safe", lambda: _FakeLogger())

    def fake_urlopen(url, *args, **kwargs):
        return io.BytesIO(
            json.dumps({"status": 500, "message": "Server exploded"}).encode("utf-8")
        )

    monkeypatch.setattr(get_datasets_module.urllib.request, "urlopen", fake_urlopen)

    with pytest.raises(ValueError, match="Server exploded"):
        get_datasets_module.get_available_datasets.fn()


def test_get_available_datasets_network_error(monkeypatch):
    """Raise ConnectionError when the HTTP request fails."""
    monkeypatch.setattr(get_datasets_module, "get_logger_safe", lambda: _FakeLogger())

    def fake_urlopen(url, *args, **kwargs):
        raise urllib.error.URLError("boom")

    monkeypatch.setattr(get_datasets_module.urllib.request, "urlopen", fake_urlopen)

    with pytest.raises(ConnectionError, match="Error connecting to server"):
        get_datasets_module.get_available_datasets.fn()


@pytest.mark.parametrize(
    "kwargs, expected_message",
    [
        ({"filter": 123}, "Filter must be a string"),
        ({"search": 456}, "Search query must be a string"),
        ({"area": 789}, "Area must be a string"),
    ],
)
def test_get_available_datasets_validates_string_inputs(kwargs, expected_message):
    """Reject non-string filter/search/area arguments."""
    with pytest.raises(ValueError, match=expected_message):
        get_datasets_module.get_available_datasets.fn(**kwargs)
