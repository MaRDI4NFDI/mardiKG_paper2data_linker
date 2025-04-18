# If you are developing locally, make sure to be on the most current version of the tools lib:
# -> pip install -e ../mardiportal-workflowtools

import getpass
import socket
import logging

from mardiportal.workflowtools import read_credentials
from prefect import flow, get_run_logger
from pathlib import Path

from tasks.ucimlrepo_link_papers_with_datasets import link_papers_with_datasets
from tasks.upload import upload_ucidump_lakefs
from tasks.ucimlrepo_get_dump import get_dump
from tasks.ucimlrepo_get_datasets import get_available_datasets
from tasks.ucimlrepo_update_dump import update_dump
from tasks.ucimlrepo_link_intropapers_with_datasets import link_intropapers_with_datasets

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

    # Check whether data directory already exists
    Path(DATA_PATH).mkdir(parents=True, exist_ok=True)
    logger.info("Using data directory: %s", DATA_PATH)

    # Get list of available datasets through API
    task = get_available_datasets.submit()
    uci_dataset_list = task.result()
    uci_dataset_ids = [d["id"] for d in uci_dataset_list]

    # Get dump - if not available locally, download or crawl
    uci_dump_filename = "uci_datasets_final.json"
    uci_dump_file_and_path = str(Path(DATA_PATH) / uci_dump_filename)
    task = get_dump.submit(
        uci_dump_file_and_path=uci_dump_file_and_path,
        uci_dataset_ids=uci_dataset_ids,
        lakefs_url=lakefs_url,
        lakefs_repo=lakefs_repo,
        lakefs_path=lakefs_path
    )
    dump_file_existed = task.result()

    # TODO: Check whether available dump should be updated
    task = update_dump.submit(
        uci_dump_file_and_path=uci_dump_file_and_path,
        uci_dataset_ids=uci_dataset_ids
    )
    dump_file_updated = task.result()

    # Link papers to datasets
#    logger.info("Processing dump: linking papers to datasets...")
#    mapping_file = str(Path(DATA_PATH) / "uci2mardi_dataset_mapping.txt")
#    link_papers_with_datasets.submit(json_input=uci_dump_file_and_path, mapping_file=mapping_file)

    # Link introductionary papers to datasets
    logger.info("Processing dump: linking introductionary papers to datasets...")
    mapping_file = str(Path(DATA_PATH) / "uci2mardi_dataset_mapping.txt")
    link_intropapers_with_datasets.submit(json_input=uci_dump_file_and_path, mapping_file=mapping_file)

    # Upload new dump file to lakeFS - if needed
    if not dump_file_existed or dump_file_updated:
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
