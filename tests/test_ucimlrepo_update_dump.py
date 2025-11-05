import asyncio
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import List

sys.path.append(str(Path(__file__).resolve().parents[1]))

if "tasks.ucimlrepo_crawl" not in sys.modules:
    fake_crawl_module = ModuleType("tasks.ucimlrepo_crawl")

    async def _placeholder_crawl_item(*args, **kwargs):
        raise RuntimeError("crawl_item should be patched in tests")

    fake_crawl_module.crawl_item = _placeholder_crawl_item
    sys.modules["tasks.ucimlrepo_crawl"] = fake_crawl_module

from tasks import ucimlrepo_update_dump as update_dump_module


class _FakeLogger:
    def __init__(self) -> None:
        self.messages: List[str] = []

    def info(self, message: str, *args, **kwargs) -> None:
        formatted = message % args if args else message
        self.messages.append(formatted)


def test_update_dump_returns_false_when_all_ids_present(tmp_path, monkeypatch):
    """Return False and leave dump untouched when all requested IDs already exist."""
    fake_logger = _FakeLogger()
    monkeypatch.setattr(update_dump_module, "get_run_logger", lambda: fake_logger)

    dump_path = tmp_path / "uci_dump.json"
    existing_entries = [
        {"dataset_id": 1, "name": "Existing 1"},
        {"dataset_id": 2, "name": "Existing 2"},
    ]
    dump_path.write_text(json.dumps(existing_entries), encoding="utf-8")

    async def fake_crawl_item(dataset_id: int):
        raise AssertionError("crawl_item should not be called when IDs are present")

    monkeypatch.setattr(update_dump_module, "crawl_item", fake_crawl_item)

    result = asyncio.run(update_dump_module.update_dump.fn(str(dump_path), [1, 2]))

    assert result is False
    updated_entries = json.loads(dump_path.read_text(encoding="utf-8"))
    assert updated_entries == existing_entries


def test_update_dump_fetches_missing_ids_and_updates_file(tmp_path, monkeypatch):
    """Crawl missing IDs, append their metadata, and report that an update occurred."""
    fake_logger = _FakeLogger()
    monkeypatch.setattr(update_dump_module, "get_run_logger", lambda: fake_logger)

    dump_path = tmp_path / "uci_dump.json"
    existing_entries = [{"dataset_id": 1, "name": "Existing"}]
    dump_path.write_text(json.dumps(existing_entries), encoding="utf-8")

    collected_ids = []

    async def fake_crawl_item(dataset_id: int):
        collected_ids.append(dataset_id)
        return {"dataset_id": dataset_id, "name": f"Fetched {dataset_id}"}

    monkeypatch.setattr(update_dump_module, "crawl_item", fake_crawl_item)

    result = asyncio.run(update_dump_module.update_dump.fn(str(dump_path), [1, 2, 3]))

    assert result is True
    assert collected_ids == [2, 3]

    updated_entries = json.loads(dump_path.read_text(encoding="utf-8"))
    assert {"dataset_id": 1, "name": "Existing"} in updated_entries
    assert {"dataset_id": 2, "name": "Fetched 2"} in updated_entries
    assert {"dataset_id": 3, "name": "Fetched 3"} in updated_entries
    assert len(updated_entries) == 3
