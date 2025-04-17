# If you are developing locally, make sure to be on the most current version of the tools lib:
# -> pip install -e ../mardiportal-workflowtools

import getpass
import socket
import logging

from prefect import flow, get_run_logger
from pathlib import Path

from tasks.ucimlrepo_crawl import start_ucimlrepo_crawl
from tasks.ucimlrepo_kg_updates import link_publications_to_datasets_in_mardi_kg
from tasks.ucimlrepo_process_dump import process_uci_ml_repo_dump
from tasks.ucimlrepo_get_kg_qids import get_dataset_qids_from_kg


from utils.logger_helper import configure_prefect_logging_to_file

# Set paths
DATA_PATH="./data" # Path where intermediate data and the database file should be stored locally.

# Set basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

@flow
def process_datasets(
    lakefs_url: str,
    lakefs_repo: str,
    lakefs_path_and_file: str,
    last_index_to_crawl=968
):
    # Configure logging
    logfile_name = "workflow.log.txt"
    configure_prefect_logging_to_file( logfile_name )
    logger = get_run_logger()
    logger.info(f"Starting workflow on system: {socket.gethostname()} by user: {getpass.getuser()}")

    # Check whether data (and path) already exists
    Path(DATA_PATH).mkdir(parents=True, exist_ok=True)
    logger.info("Using data directory: %s", DATA_PATH)
    uci_dump_file = str(Path(DATA_PATH) / "uci_datasets_final.json")

    # If data does not yet exist -> start crawl
    if Path(uci_dump_file).is_file():
        logger.info("UCI dump file exists: %s", uci_dump_file)
    else:
        logger.warning(f"UCI dump file '{uci_dump_file}' does not exist - "
                       "starting crawl until # {last_index_to_crawl} ...")

        # Start crawl
        start_ucimlrepo_crawl.submit(
            uci_dump_file=uci_dump_file, last_index_to_crawl=last_index_to_crawl
        ).wait()

    # Process dump
    logger.info("Processing dump...")
    task = process_uci_ml_repo_dump.submit(json_input=uci_dump_file)
    datasets_with_citations_available_in_mardi = task.result()

    # Link entries to available datasets in the KG
    logger.info("Linking to KG datasets")
    mapping_file = str(Path(DATA_PATH) / "uci2mardi_dataset_mapping.txt")
    task = get_dataset_qids_from_kg.submit(
        entries=datasets_with_citations_available_in_mardi, mapping_file=mapping_file)
    hits = task.result()

    for entry in hits:
        print(f"\nDataset: {entry['dataset_name']} (ID: {entry['dataset_id']})")
        print(f"MaRDI dataset QID: {entry.get('dataset_mardi_QID')}")
        print(f"Publication arXiv ID: {entry.get('arxiv_id')}")
        print(f"Publication QID: {entry.get('publication_mardi_QID')}")
        print("Citations with arXiv IDs:")

        for citation in entry.get("citations", []):
            if citation.get("arxiv"):
                print(f"  - {citation['title']} (arXiv: {citation['arxiv']})")


     #link_publications_to_datasets_in_mardi_kg( hits )

    logger.info("Workflow complete.")



if __name__ == "__main__":
    process_datasets(
        lakefs_url="https://lake-bioinfmed.zib.de",
        lakefs_repo="mardi-workflows-files",
        lakefs_path_and_file="mardiKG_paper2data_linker/results.db",
        last_index_to_crawl=968
    )
