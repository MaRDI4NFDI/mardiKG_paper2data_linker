# If you are developing locally, make sure to be on the most current version of the tools lib:
# -> pip install -e ../mardiportal-workflowtools

import getpass
import socket
import logging

from mardiportal.workflowtools import read_credentials
from prefect import flow, get_run_logger
from pathlib import Path

from tasks.ucimlrepo_kg_updates import link_publications_to_datasets_in_mardi_kg
from tasks.ucimlrepo_process_dump import process_uci_ml_repo_dump
from tasks.ucimlrepo_get_kg_qids import get_dataset_qids_from_kg
from tasks.upload import upload_ucidump_lakefs
from tasks.ucimlrepo_get_dump import get_dump
from tasks.ucimlrepo_get_datasets import get_available_datasets

from utils.logger_helper import configure_prefect_logging_to_file

from mardiportal.workflowtools.lake_client import upload_and_commit_to_lakefs

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
    lakefs_path: str
):

    # Configure logging
    logfile_name = "workflow.log.txt"
    configure_prefect_logging_to_file( logfile_name )
    logger = get_run_logger()
    logger.info(f"Starting workflow on system: {socket.gethostname()} by user: {getpass.getuser()}")

    # Check whether data (and path) already exists
    Path(DATA_PATH).mkdir(parents=True, exist_ok=True)
    logger.info("Using data directory: %s", DATA_PATH)

    # Get list of available datasets through API
    task = get_available_datasets.submit()
    uci_dataset_list = task.result()

    # Get dump - if not available locally, download or crawl
    uci_dump_filename = "uci_datasets_final.json"
    uci_dump_file_and_path = str(Path(DATA_PATH) / uci_dump_filename)
    task = get_dump.submit(
        uci_dump_file_and_path=uci_dump_file_and_path,
        uci_dataset_list=uci_dataset_list,
        lakefs_url=lakefs_url,
        lakefs_repo=lakefs_repo,
        lakefs_path=lakefs_path
    )
    dump_file_existed = task.result()

    # Process dump
    logger.info("Processing dump...")
    task = process_uci_ml_repo_dump.submit(json_input=uci_dump_file_and_path)
    datasets_with_citations_available_in_mardi = task.result()

    # Add dataset QIDs to entries if the datasets exist in the KG
    # Returns only entries for which a dataset QID exists
    logger.info("Linking to KG datasets")
    mapping_file = str(Path(DATA_PATH) / "uci2mardi_dataset_mapping.txt")
    task = get_dataset_qids_from_kg.submit(
        entries=datasets_with_citations_available_in_mardi, mapping_file=mapping_file)
    hits = task.result()

    # Perform linking in KG
    link_publications_to_datasets_in_mardi_kg( hits )

    # Upload new dump file to lakeFS - if needed
    if not dump_file_existed:
        logger.info("Upload new dump file to lakeFS...")
        upload_ucidump_lakefs.submit(
            path_and_file=str(uci_dump_file_and_path),
            lakefs_url=lakefs_url,
            lakefs_repo=lakefs_repo,
            lakefs_path=lakefs_path,
            msg="Upload new version of dump file"
        ).wait()

    # Upload logfile to lakeFS
    creds = read_credentials("lakefs")
    if creds:
        logger.info("Upload logfile to lakeFS...")
        upload_and_commit_to_lakefs(
            path_and_file=logfile_name,
            lakefs_url=lakefs_url,
            lakefs_repo=lakefs_repo,
            lakefs_path=lakefs_path,
            msg="Logfile from current run",
            lakefs_user=creds["user"],
            lakefs_pwd=creds["password"],
        )
    else:
        logger.error("No valid credentials found for lakeFS. Skipping upload.")

    logger.info("Workflow complete.")



if __name__ == "__main__":
    process_datasets(
        lakefs_url="https://lake-bioinfmed.zib.de",
        lakefs_repo="mardi-workflows-files",
        lakefs_path="mardiKG_paper2data_linker/"
    )
