import logging
import sqlite3
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict

from mardiportal.workflowtools import read_credentials
from prefect import task, get_run_logger
from mardiclient import MardiClient, MardiItem
from wikibaseintegrator import datatypes
from wikibaseintegrator.models import References, Reference
from wikibaseintegrator.wbi_enums import ActionIfExists


@task
def link_publications_to_datasets_in_mardi_kg( hits: List[Dict], secrets_path: str = "secrets.conf") -> None:

    logger = get_run_logger()

    # Read username/password from file
    creds = read_credentials("mardi-kg", secrets_path)
    if not creds:
        logger.error("No valid credentials found. Please check '%s'", secrets_path)
        return

    # Initialize MaRDI KG client
    mc = MardiClient(user=creds["user"], password=creds["password"], login_with_bot=True)

    # Get items to be updated from the database
    logger.info("Loaded %d items pending MaRDI update", len(hits))

    # Show list of QIDs that will be updated
    hits_qids = ", ".join(hit.get("qid", "?") for hit in hits)
    logger.info("QIDs to be updated: %s", hits_qids)

    for hit in hits:
        _process_hit( hit, mc)

    logger.info("Finished updating %d items in %.2f seconds", len(hits), duration)


def _process_hit(hit: Dict, mc: MardiClient) -> None:
    """Process a single "hit" entry. This updates the KG item.

    Args:
        hit (Dict): A dictionary containing hit information (qid, repo_url, etc.).
        mc (MardiClient): An authenticated MaRDI KG client instance.
    """

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    qid = hit.get("qid")
    repo_url = hit.get("repo_url")
    pwc_url = hit.get("pwc_page")
    mentioned_in_paper = hit.get("mentioned_in_paper", False)
    mentioned_in_github = hit.get("mentioned_in_github", False)

    harvested_from = (
        "publication" if mentioned_in_paper else
        "repository README" if mentioned_in_github else
        "unknown"
    )

    # Update actual KG item and update item in local DB
    if qid and repo_url and pwc_url:
        logger.info(f"Linking {qid} with {repo_url}")
        _update_kg_item_with_repo(mc, qid, repo_url, pwc_url, harvested_from)
        _mark_updated(db_path, hit["arxiv_id"])
    else:
        logger.warning(f"Skipping due to missing fields: {hit}")


def _load_hits(db_path: str) -> List[Dict]:
    """Load unprocessed hits from the SQLite DB.

    Args:
        db_path (str): Path to the SQLite database.

    Returns:
        List[Dict]: List of hit entries that have not yet been marked as updated.
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT * FROM hits WHERE updated_in_mardi_kg = 0 AND qid IS NOT NULL")
        rows = cur.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    except Exception as e:
        get_run_logger().error(f"Error reading hits from DB: {e}")
        return []


def _mark_updated(db_path: str, arxiv_id: str) -> None:
    """Mark a specific arXiv ID entry as updated in the SQLite database.

    Args:
        db_path (str): Path to the SQLite database.
        arxiv_id (str): The arXiv ID of the hit to mark as updated.
    """
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE hits
            SET updated_in_mardi_kg = 1,
                timestamp_added_to_mardikg = ?
            WHERE arxiv_id = ?
            """,
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), arxiv_id)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        get_run_logger().error(f"Error updating flag for {arxiv_id}: {e}")


def _update_kg_item_with_repo(mc: MardiClient, QID: str, repo_url: str,
                              repo_reference_url: str, harvested_from_label: str) -> None:
    """Add or update a repository link (P1687) on a MaRDI KG item.

    Args:
        mc (MardiClient): Authenticated MardiClient instance.
        QID (str): QID of the MaRDI KG item.
        repo_url (str): URL of the companion code repository.
        repo_reference_url (str): PapersWithCode reference page URL.
        harvested_from_label (str): Description of the source ('publication', 'README', etc.).
    """
    item: MardiItem = mc.item.get(entity_id=QID)

    # Prepare reference:
    #   - Reference: P1688 (PapersWithCode reference URL) = repo_reference_url
    #   - Reference: P1689 (extracted from) = harvested_from_label
    # See also: https://github.com/LeMyst/WikibaseIntegrator?tab=readme-ov-file#manipulate-claim-add-references
    new_references = References()
    new_reference = Reference()

    new_reference.add(datatypes.String(prop_nr='P1688', value=repo_reference_url))
    new_reference.add(datatypes.String(prop_nr='P1689', value=harvested_from_label))

    new_references.add(new_reference)

    # Prepare statement:
    #   - Property: P1687 (has companion code repository)
    #   - Value: url_to_repo
    new_claim = datatypes.String(
        prop_nr='P1687',
        value=repo_url,
        references=new_references
    )

    # Add the claim
    item.claims.add(new_claim, action_if_exists=ActionIfExists.REPLACE_ALL)

    # Write the new data
    item.write()
