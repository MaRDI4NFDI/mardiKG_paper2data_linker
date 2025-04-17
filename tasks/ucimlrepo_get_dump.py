from pathlib import Path
from typing import List

from mardiportal.workflowtools import read_credentials, LakeClient
from prefect import task, get_run_logger

from tasks.ucimlrepo_crawl import start_ucimlrepo_crawl

@task
def get_dump(
        uci_dump_file_and_path: str, uci_dataset_ids: List[int],
        lakefs_url: str, lakefs_repo: str, lakefs_path: str) -> bool:

    logger = get_run_logger()
    uci_dump_filename = Path(uci_dump_file_and_path).name

    # Check whether dump file exists
    if Path(uci_dump_file_and_path).is_file():
        logger.info(f"Using existing DB file at {uci_dump_file_and_path}")
        return True

    # Try to download UCI dump file from lakeFS
    logger.warning(f"UCI dump file not found at {uci_dump_file_and_path}, trying to download...")
    got_dump_from_lakefs = _download_ucidump_lakefs(
        dump_path_and_file=str(uci_dump_file_and_path),
        lakefs_url=lakefs_url,
        lakefs_repo=lakefs_repo,
        lakefs_path_and_file=lakefs_path + uci_dump_filename
    )
    if got_dump_from_lakefs:
        return True

    # If data does not yet exist and could not be downloaded from lakeFS
    # -> start full crawl, based on available dataset IDs
    logger.warning(f"UCI dump file '{uci_dump_file_and_path}' still does not exist - "
                   "starting full crawl ...")

    # Start full crawl with IDs from the API call
    start_ucimlrepo_crawl.submit(
        uci_dump_file=uci_dump_file_and_path, dataset_id_list=uci_dataset_ids
    ).wait()

    return False


def _download_ucidump_lakefs(
        dump_path_and_file, lakefs_url: str, lakefs_repo: str,
        lakefs_path_and_file:str, secrets_path: str = "secrets.conf") -> bool:
    """
    Downloads the dump file from a previous run of the flow from a lakeFS repository and
    saves it locally.

    Args:
        dump_path_and_file (str): The local file path (including filename) where the database should be saved.
        lakefs_url (str): The URL of the lakeFS server to connect to.
        lakefs_repo (str): The name of the lakeFS repository where the file is stored.
        lakefs_path_and_file (str): The path (in lakeFS) to the file to be downloaded.
        secrets_path (str, optional): Path to a fallback local secrets file if Prefect block secrets are unavailable. Defaults to "secrets.conf".

    Raises:
        Exception: If the file exists in lakeFS but content could not be retrieved.
        FileNotFoundError: If the file does not exist in the specified lakeFS repository.
    """
    logger = get_run_logger()

    creds = read_credentials("lakefs", secrets_path)
    if not creds:
        logger.error("No valid credentials found. Please check '%s'", secrets_path)
        return False

    # Initialize LakeFS client
    lakefs_user = creds["user"]
    lakefs_pwd = creds["password"]
    client = LakeClient(lakefs_url, lakefs_user, lakefs_pwd)

    if client.file_exists(lakefs_repo, "main", lakefs_path_and_file):
        logger.info("Found DB file at lakeFS. Downloading...")
        content = client.load_file(lakefs_repo, "main", lakefs_path_and_file)
        if not content:
            logger.error("Failed downloading DB file from lakeFS.")
            return False

    # Save content to local file
    dump_file_path = Path(dump_path_and_file)
    with open(dump_file_path, "wb") as f:
        f.write(content)

    logger.info("Successfully saved DB file to '%s'", dump_file_path)

    return True
