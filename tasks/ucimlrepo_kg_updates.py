import logging
from typing import List, Dict

from mardiportal.workflowtools import read_credentials
from prefect import task, get_run_logger
from mardiclient import MardiClient, MardiItem
from wikibaseintegrator import datatypes
from wikibaseintegrator.models import References, Reference
from wikibaseintegrator.wbi_enums import ActionIfExists


@task
def link_publications_to_datasets_in_mardi_kg(
        hits: List[Dict], secrets_path: str = "secrets.conf") -> None:
    """Links publications to datasets in the MaRDI Knowledge Graph.

    For each entry, this function adds a `P223` (cites work) claim to the
    publication's item pointing to the corresponding dataset QID, and
    adds a `P1689` (extracted from) reference pointing to the original UCI dataset URL.

    Args:
        hits (List[Dict]): A list of dictionaries, each representing a publication-dataset link
        secrets_path (str, optional): Path to a secrets file containing
                                      login credentials for the bot that changes the KG.
    """
    logger = get_run_logger()

    # Read username/password from file
    creds = read_credentials("mardi-kg", secrets_path)
    if not creds:
        logger.error("No valid credentials found. Please check '%s'", secrets_path)
        return

    # Initialize MaRDI KG client
    mc = MardiClient(user=creds["user"], password=creds["password"], login_with_bot=True)

    # Show list of QIDs that will be updated
    logger.info("Will update %d items in the MaRDI KG", len(hits))
    hits_qids = ", ".join(hit.get("publication_mardi_QID", "?") for hit in hits)
    logger.info("QIDs (publications) to be updated: %s", hits_qids)

    # Perform the linking for all items
    for hit in hits:
        _process_hit( hit, mc)

    logger.info("Finished updating %d items", len(hits))


def _process_hit(hit: Dict, mc: MardiClient) -> None:
    """Performs the linking in the MaRDI KG.

    Args:
        hit (Dict): A dictionary containing:
            - 'publication_mardi_QID': QID of the publication item.
            - 'dataset_mardi_QID': QID of the dataset item.
            - 'dataset_uci_url': The URL from the UCI repository.
        mc (MardiClient): An authenticated instance of the MaRDI client used to access the KG.
    """

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    dataset_uci_url = hit['dataset_url']
    dataset_mardi_QID = hit['dataset_mardi_QID']
    publication_mardi_QID = hit['publication_mardi_QID']

    # Update actual KG item and update item in local DB
    if dataset_mardi_QID and publication_mardi_QID:
        logger.info(f"Linking publication {publication_mardi_QID} with dataset {dataset_mardi_QID}")
        _update_kg_item_with_repo(mc, publication_mardi_QID, dataset_mardi_QID, dataset_uci_url)
    else:
        logger.warning(f"Skipping due to missing fields: {hit}")

def _update_kg_item_with_repo(
        mc: MardiClient,
        publication_mardi_QID: str, dataset_mardi_QID: str, dataset_uci_url: str) -> None:
    item: MardiItem = mc.item.get(entity_id=publication_mardi_QID)

    # Prepare reference:
    #   - P1689 (extracted from)
    # See also: https://github.com/LeMyst/WikibaseIntegrator?tab=readme-ov-file#manipulate-claim-add-references
    new_references = References()
    new_reference = Reference()
    new_reference.add(datatypes.String(prop_nr='P1689', value=dataset_uci_url))
    new_references.add(new_reference)

    # Prepare statement:
    #   - Property: P223 (cites work)
    #   - Value: url_to_repo
    new_claim = datatypes.Item(
        prop_nr='P223',
        value=dataset_mardi_QID,
        references=new_references
    )

    # Add the claim
    item.claims.add(new_claim, action_if_exists=ActionIfExists.APPEND_OR_REPLACE)

    # Write the new data
    item.write()
