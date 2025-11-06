import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from tasks import ucimlrepo_get_dump as get_dump_module


class _FakeLogger:
    def __init__(self) -> None:
        self.messages: List[str] = []

    def info(self, message: str, *args, **kwargs) -> None:
        formatted = message % args if args else message
        self.messages.append(formatted)

    def warning(self, message: str, *args, **kwargs) -> None:
        formatted = message % args if args else message
        self.messages.append(formatted)

    def error(self, message: str, *args, **kwargs) -> None:
        formatted = message % args if args else message
        self.messages.append(formatted)


@pytest.fixture
def fake_logger(monkeypatch) -> _FakeLogger:
    logger = _FakeLogger()
    monkeypatch.setattr(get_dump_module, "get_run_logger", lambda: logger)
    return logger


def test_get_dump_returns_true_when_local_file_exists(tmp_path, monkeypatch, fake_logger):
    """Return True immediately if the requested dump already exists on disk."""
    dump_path = tmp_path / "uci_dump.sqlite"
    dump_path.write_bytes(b"existing-data")

    monkeypatch.setattr(
        get_dump_module,
        "_download_ucidump_lakefs",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("_download_ucidump_lakefs should not run when file exists")
        ),
    )

    class _NeverRunTask:
        def submit(self, *args, **kwargs):
            raise AssertionError("start_ucimlrepo_full_crawl should not be invoked")

    monkeypatch.setattr(get_dump_module, "start_ucimlrepo_full_crawl", _NeverRunTask())

    result = get_dump_module.get_dump.fn(
        str(dump_path), [1, 2], "https://lakefs", "repo", "path/"
    )

    assert result is True


def test_get_dump_downloads_from_lakefs_when_missing(tmp_path, monkeypatch, fake_logger):
    """Attempt to fetch from lakeFS if the dump is missing locally."""
    dump_path = tmp_path / "uci_dump.sqlite"
    called_with: Dict[str, Any] = {}

    def _fake_download(*, dump_path_and_file, lakefs_url, lakefs_repo, lakefs_path_and_file):
        called_with["dump_path_and_file"] = dump_path_and_file
        called_with["lakefs_url"] = lakefs_url
        called_with["lakefs_repo"] = lakefs_repo
        called_with["lakefs_path_and_file"] = lakefs_path_and_file
        dump_path.write_bytes(b"downloaded")
        return True

    monkeypatch.setattr(get_dump_module, "_download_ucidump_lakefs", _fake_download)

    class _NeverRunTask:
        def submit(self, *args, **kwargs):
            raise AssertionError("Full crawl should not run when download succeeds")

    monkeypatch.setattr(get_dump_module, "start_ucimlrepo_full_crawl", _NeverRunTask())

    result = get_dump_module.get_dump.fn(
        str(dump_path),
        [1, 2],
        "https://lakefs",
        "repo",
        "path/",
    )

    assert result is True
    assert called_with == {
        "dump_path_and_file": str(dump_path),
        "lakefs_url": "https://lakefs",
        "lakefs_repo": "repo",
        "lakefs_path_and_file": "path/" + dump_path.name,
    }


def test_get_dump_runs_full_crawl_after_failed_download(tmp_path, monkeypatch, fake_logger):
    """Kick off the fallback crawl if download fails."""
    dump_path = tmp_path / "uci_dump.sqlite"
    uci_ids = [42, 43]

    monkeypatch.setattr(get_dump_module, "_download_ucidump_lakefs", lambda *args, **kwargs: False)

    class _FakeSubmission:
        def __init__(self) -> None:
            self.wait_called = False

        def wait(self) -> None:
            self.wait_called = True

    class _FakeStartTask:
        def __init__(self) -> None:
            self.submitted_args = None
            self.submitted_kwargs = None
            self.submission = _FakeSubmission()

        def submit(self, *args, **kwargs):
            self.submitted_args = args
            self.submitted_kwargs = kwargs
            return self.submission

    fake_start = _FakeStartTask()
    monkeypatch.setattr(get_dump_module, "start_ucimlrepo_full_crawl", fake_start)

    result = get_dump_module.get_dump.fn(
        str(dump_path), uci_ids, "https://lakefs", "repo", "path/"
    )

    assert result is False
    assert fake_start.submitted_kwargs == {
        "uci_dump_file": str(dump_path),
        "dataset_id_list": uci_ids,
    }
    assert fake_start.submission.wait_called is True


def test_download_returns_false_when_credentials_missing(tmp_path, monkeypatch, fake_logger):
    """Gracefully bail out if no lakeFS credentials can be loaded."""
    monkeypatch.setattr(get_dump_module, "read_credentials", lambda *args, **kwargs: None)

    result = get_dump_module._download_ucidump_lakefs(
        dump_path_and_file=str(tmp_path / "db.sqlite"),
        lakefs_url="https://lakefs",
        lakefs_repo="repo",
        lakefs_path_and_file="path/file",
    )

    assert result is False


def test_download_returns_false_when_content_missing(tmp_path, monkeypatch, fake_logger):
    """Return False and avoid writing a file if lakeFS returns empty content."""

    def _fake_read_credentials(*args, **kwargs):
        return {"user": "user", "password": "pwd"}

    class _FakeLakeClient:
        def __init__(self, url, user, pwd) -> None:
            self.url = url
            self.user = user
            self.pwd = pwd

        def file_exists(self, repo, branch, path):
            return True

        def load_file(self, repo, branch, path):
            return b""

    monkeypatch.setattr(get_dump_module, "read_credentials", _fake_read_credentials)
    monkeypatch.setattr(get_dump_module, "LakeClient", _FakeLakeClient)

    target_path = tmp_path / "db.sqlite"
    result = get_dump_module._download_ucidump_lakefs(
        dump_path_and_file=str(target_path),
        lakefs_url="https://lakefs",
        lakefs_repo="repo",
        lakefs_path_and_file="path/file",
    )

    assert result is False
    assert target_path.exists() is False


def test_download_saves_file_when_content_available(tmp_path, monkeypatch, fake_logger):
    """Write the downloaded bytes to disk and report success."""

    def _fake_read_credentials(*args, **kwargs):
        return {"user": "user", "password": "pwd"}

    class _FakeLakeClient:
        def __init__(self, url, user, pwd) -> None:
            self.url = url
            self.user = user
            self.pwd = pwd

        def file_exists(self, repo, branch, path):
            return True

        def load_file(self, repo, branch, path):
            return b"content"

    monkeypatch.setattr(get_dump_module, "read_credentials", _fake_read_credentials)
    monkeypatch.setattr(get_dump_module, "LakeClient", _FakeLakeClient)

    target_path = tmp_path / "db.sqlite"
    result = get_dump_module._download_ucidump_lakefs(
        dump_path_and_file=str(target_path),
        lakefs_url="https://lakefs",
        lakefs_repo="repo",
        lakefs_path_and_file="path/file",
    )

    assert result is True
    assert target_path.read_bytes() == b"content"
