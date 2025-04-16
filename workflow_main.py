# If you are developing locally, make sure to be on the most current version of the tools lib:
# -> pip install -e ../mardiportal-workflowtools

import getpass
import socket
import logging

from prefect import flow, get_run_logger
from pathlib import Path

from tasks.ucimlrepo_crawl import start_ucimlrepo_crawl
from tasks.ucimlrepo_process_dump import process_uci_ml_repo_dump
from tasks.ucimlrepo_get_kg_qids import get_kg_qids


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
    lakefs_path_and_file: str
):
    # Configure logging
    logfile_name = "workflow.log.txt"
    configure_prefect_logging_to_file( logfile_name )
    logger = get_run_logger()
    logger.info(f"Starting workflow on system: {socket.gethostname()} by user: {getpass.getuser()}")

    # Check whether data (and path) already exists
    Path(DATA_PATH).mkdir(parents=True, exist_ok=True)
    logger.info("Using data directory: %s", DATA_PATH)
    json_input = str(Path(DATA_PATH) / "uci_datasets_final.json")

    # If data does not yet exist -> start crawl
    if Path(json_input).is_file():
        logger.info("Data file exists: %s", json_input)
    else:
        logger.warning("Data file does not exist: %s - starting crawl ...", json_input)

        # Start crawl
        start_ucimlrepo_crawl.submit(
            json_input=json_input
        ).wait()

    # Process dump
    logger.info("Processing dump...")
    task = process_uci_ml_repo_dump.submit(json_input=json_input)
    entries = task.result()

    # Link entries to available datasets in the KG
    logger.info("Linking to KG datasets")
    mapping_file = str(Path(DATA_PATH) / "uci2mardi_dataset_mapping.txt")
    task = get_kg_qids.submit(entries=entries, mapping_file=mapping_file)
    result = task.result()

    print( result )

    logger.info("Workflow complete.")



if __name__ == "__main__":
    process_datasets(
        lakefs_url="https://lake-bioinfmed.zib.de",
        lakefs_repo="mardi-workflows-files",
        lakefs_path_and_file="mardiKG_paper2data_linker/results.db"
    )
