import os
from mardiclient import MardiClient, MardiItem
from wikibaseintegrator import datatypes
from wikibaseintegrator.models import References, Reference
from wikibaseintegrator.wbi_enums import ActionIfExists
import pytest


def read_secrets():
    """Read bot credentials from ../secrets.conf if present."""
    secrets_path = os.path.join(os.path.dirname(__file__), "..", "secrets.conf")
    if not os.path.isfile(secrets_path):
        return None, None

    creds = {}
    with open(secrets_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, value = line.split("=", 1)
                creds[key.strip()] = value.strip()

    user = creds.get("mardi-kg-user")
    password = creds.get("mardi-kg-password")
    return user, password


@pytest.mark.skipif(
    not os.path.isfile(os.path.join(os.path.dirname(__file__), "..", "secrets.conf")),
    reason="../secrets.conf not found"
)
def test_add_claim_to_dummy_item():
    user, password = read_secrets()
    if not user or not password:
        pytest.skip("Missing credentials in ../secrets.conf")

    QID = "Q5422210"
    print(f"\nRunning 'test_add_claim_to_dummy_item()' for QID '{QID}' ...")

    mc = MardiClient(user=user, password=password, login_with_bot=True)
    item: MardiItem = mc.item.get(entity_id=QID)

    print(f"\nGot item: {item}")

    # Prepare reference
    new_references = References()
    new_reference = Reference()
    new_reference.add(datatypes.String(prop_nr="P1689", value="my dummy source"))
    new_references.add(new_reference)

    print(f"\nNow changing P223 ...")

    # Prepare statement
    new_claim = datatypes.Item(
        prop_nr="P223",
        value=QID,
        references=new_references,
    )

    item.claims.add(new_claim, action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

    print(f"\nSaving item ...")
    item.write()
    print(f"\nDone.")
