from mardiportal.workflowtools import read_credentials, LakeClient, IPFSClient, \
    upload_and_commit_to_lakefs
from prefect import task, get_run_logger
from mardiportal.workflowtools.lake_client import upload_and_commit_to_lakefs

@task
def upload_ucidump_lakefs( path_and_file: str,
                         lakefs_url: str, lakefs_repo: str, lakefs_path:str,
                         msg: str = "Not commit message",
                         secrets_path: str = "secrets.conf" ) -> None:
    """
    Uploads a local database file to a specified path in a lakeFS repository and commits the upload.

    This function reads lakeFS credentials from a secrets file, initializes a LakeClient,
    uploads the file to the given lakeFS path, and creates a commit in the 'main' branch.

    Args:
        path_and_file (str): The local file path (including filename) to upload.
        lakefs_url (str): The URL of the lakeFS instance.
        lakefs_repo (str): The name of the lakeFS repository to upload to.
        lakefs_path (str): The destination path in the lakeFS repository (no file name).
        msg (str): The commit message.
        secrets_path (str, optional): Path to the secrets configuration file containing lakeFS credentials.
            Defaults to "secrets.conf".

    Returns:
        None

    Raises:
        Logs an error and exits early if credentials cannot be read.
    """

    logger = get_run_logger()

    creds = read_credentials("lakefs", secrets_path)
    if not creds:
        logger.error("No valid credentials found. Please check '%s'", secrets_path)
        return

    logger.info(f"Uploading {path_and_file} to lakeFS ({lakefs_repo} -> main -> {lakefs_path})")

    upload_and_commit_to_lakefs(
        path_and_file=path_and_file,
        lakefs_url=lakefs_url,
        lakefs_repo=lakefs_repo,
        lakefs_path=lakefs_path,
        msg=msg,
        lakefs_user=creds["user"],
        lakefs_pwd=creds["password"],
    )
