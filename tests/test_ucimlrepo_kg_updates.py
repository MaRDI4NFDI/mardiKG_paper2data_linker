from pathlib import Path
import sys
from types import ModuleType
from typing import List, Dict

sys.path.append(str(Path(__file__).resolve().parents[1]))

fake_mardiportal = ModuleType("mardiportal")
fake_workflowtools = ModuleType("mardiportal.workflowtools")


class _PlaceholderLakeClient:
    pass


def _placeholder_read_credentials(*args, **kwargs):
    raise RuntimeError("read_credentials should be patched in tests")


fake_workflowtools.LakeClient = _PlaceholderLakeClient
fake_workflowtools.read_credentials = _placeholder_read_credentials

sys.modules["mardiportal"] = fake_mardiportal
sys.modules["mardiportal.workflowtools"] = fake_workflowtools

fake_mardiclient = ModuleType("mardiclient")


class _PlaceholderMardiClient:
    def __init__(self, *args, **kwargs):
        raise RuntimeError("MardiClient should be patched in tests")


class _PlaceholderMardiItem:
    pass


fake_mardiclient.MardiClient = _PlaceholderMardiClient
fake_mardiclient.MardiItem = _PlaceholderMardiItem

sys.modules["mardiclient"] = fake_mardiclient

from tasks import ucimlrepo_kg_updates as kg_updates


class _FakeLogger:
    def __init__(self) -> None:
        self.messages: List[Dict] = []

    def info(self, *args, **kwargs) -> None:
        self.messages.append({"level": "info", "args": args, "kwargs": kwargs})

    def warning(self, *args, **kwargs) -> None:
        self.messages.append({"level": "warning", "args": args, "kwargs": kwargs})

    def error(self, *args, **kwargs) -> None:
        self.messages.append({"level": "error", "args": args, "kwargs": kwargs})


def test_link_publications_to_datasets_adds_p223_claim(monkeypatch):
    """Ensure the workflow adds a P223 claim referencing the dataset entity.

    Args:
        monkeypatch: Pytest fixture used to replace external dependencies.
    """
    fake_logger = _FakeLogger()
    monkeypatch.setattr(kg_updates, "get_run_logger", lambda: fake_logger)

    def fake_read_credentials(namespace: str, secrets_path: str):
        assert namespace == "mardi-kg"
        assert secrets_path == "secrets.conf"
        return {"user": "bot", "password": "secret"}

    monkeypatch.setattr(kg_updates, "read_credentials", fake_read_credentials)

    captured_claims = []
    item_written = {"wrote": False}
    requested_entities = []
    client_inits = []

    class FakeClaims:
        def add(self, claim, action_if_exists):
            captured_claims.append((claim, action_if_exists))

    class FakeItem:
        def __init__(self):
            self.claims = FakeClaims()

        def write(self):
            item_written["wrote"] = True

    class FakeItemEndpoint:
        def get(self, entity_id: str):
            requested_entities.append(entity_id)
            return FakeItem()

    class FakeClient:
        def __init__(self, user: str, password: str, login_with_bot: bool):
            client_inits.append((user, password, login_with_bot))
            self.item = FakeItemEndpoint()

    monkeypatch.setattr(kg_updates, "MardiClient", FakeClient)

    hit = {
        "dataset_url": "https://example.org/uci-dataset",
        "dataset_mardi_QID": "Q456",
        "publication_mardi_QID": "Q123",
    }

    kg_updates.link_publications_to_datasets_in_mardi_kg.fn([hit])

    assert client_inits == [("bot", "secret", True)]
    assert requested_entities == ["Q123"]
    assert item_written["wrote"] is True
    assert len(captured_claims) == 1

    claim, action = captured_claims[0]
    claim_payload = claim.get_json()
    assert action == kg_updates.ActionIfExists.APPEND_OR_REPLACE
    assert claim_payload["mainsnak"]["property"] == "P223"
    assert claim_payload["mainsnak"]["datavalue"]["value"]["id"] == "Q456"
    references = claim_payload["references"]
    assert references[0]["snaks"]["P1689"][0]["datavalue"]["value"] == "https://example.org/uci-dataset"

