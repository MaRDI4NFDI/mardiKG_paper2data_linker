from prefect import task, get_run_logger
from pathlib import Path

from mardiportal.workflowtools import LakeClient, read_credentials, IPFSClient

@task
def download_ucidump_lakefs(
        dump_path_and_file, lakefs_url: str, lakefs_repo: str,
        lakefs_path_and_file:str, secrets_path: str = "secrets.conf") -> None:
    """
    Downloads the dump file from a previous run of the flow from a lakeFS repository and
    saves it locally.

    This function checks whether the specified file exists in the given lakeFS repository and,
    if found, downloads its content and writes it to the provided local path. Credentials are
    retrieved from Prefect secret blocks or a local secrets file.

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
        return

    # Initialize LakeFS client
    lakefs_user = creds["user"]
    lakefs_pwd = creds["password"]
    client = LakeClient(lakefs_url, lakefs_user, lakefs_pwd)

    if client.file_exists(lakefs_repo, "main", lakefs_path_and_file):
        logger.info("Found DB file at lakeFS. Downloading...")
        content = client.load_file(lakefs_repo, "main", lakefs_path_and_file)
        if not content:
            logger.error("Failed downloading DB file from lakeFS.")
            raise Exception("Failed downloading DB file from lakeFS.")

    # Save content to local file
    dump_file_path = Path(dump_path_and_file)
    with open(dump_file_path, "wb") as f:
        f.write(content)

    logger.info("Successfully saved DB file to '%s'", dump_file_path)
