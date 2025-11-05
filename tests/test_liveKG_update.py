from mardiclient import MardiClient, MardiItem
from wikibaseintegrator import datatypes
from wikibaseintegrator.models import References, Reference
from wikibaseintegrator.wbi_enums import ActionIfExists

def test_add_claim_to_dummy_item():

    QID = "Q123"

    print( f"\nRunning 'test_add_claim_to_dummy_item()' for QID '{QID}' ..." )

    mc = MardiClient(user="mybotname", password="mybotpwd", login_with_bot=True)
    item: MardiItem = mc.item.get(entity_id=QID)

    print( f"\nGot item: {item} " )

    # Prepare reference:
    #   - P1689 (extracted from)
    new_references = References()
    new_reference = Reference()
    new_reference.add(datatypes.String(prop_nr='P1689', value="my dummy source"))
    new_references.add(new_reference)

    # Prepare statement:
    #   - Property: P223 (cites work)
    new_claim = datatypes.Item(
        prop_nr='P223',
        value=QID,
        references=new_references
    )

    # Add the claim
    item.claims.add(new_claim, action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

    # Write the new data
    item.write()

