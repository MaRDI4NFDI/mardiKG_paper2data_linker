from mardiportal.workflowtools import read_credentials
from prefect import task, get_run_logger
from mardiportal.workflowtools.lake_client import upload_and_commit_to_lakefs


@task
def upload_artifacts(
        dump_file_existed: bool, dump_file_updated: bool,
        uci_dump_file_and_path: str,
        logfile_name: str,
        secrets_path: str,
        lakefs_url: str,
        lakefs_repo: str,
        lakefs_path: str
):
    """
    Uploads dump file and logfile to a lakeFS repository.

    The dump file is uploaded only if it did not previously exist or was updated
    during the current run. The logfile is always uploaded.

    Args:
        dump_file_existed (bool): Whether the dump file existed before this run.
        dump_file_updated (bool): Whether the dump file was changed during this run.
        uci_dump_file_and_path (str): Full path to the local UCI dataset dump file.
        logfile_name (str): Full path to the local logfile to be uploaded.
        secrets_path (str): Path to the secrets configuration file containing lakeFS credentials.
        lakefs_url (str): Base URL of the lakeFS instance.
        lakefs_repo (str): Name of the lakeFS repository.
        lakefs_path (str): Target path within the repository (under the 'main' branch).

    Returns:
        None
    """

    logger = get_run_logger()

    creds = read_credentials("lakefs", secrets_path)
    if not creds:
        logger.error("No valid credentials found. Please check '%s'", secrets_path)
        return

    # Upload new dump file to lakeFS - if needed
    if not dump_file_existed or dump_file_updated:
        logger.info("Upload new dump file to lakeFS...")
        logger.info(f"Uploading {uci_dump_file_and_path} to lakeFS ({lakefs_repo} -> main -> {lakefs_path})")

        upload_and_commit_to_lakefs(
            path_and_file=uci_dump_file_and_path,
            lakefs_url=lakefs_url,
            lakefs_repo=lakefs_repo,
            lakefs_path=lakefs_path,
            msg="Upload new version of dump file",
            lakefs_user=creds["user"],
            lakefs_pwd=creds["password"],
        )

    # Upload logfile to lakeFS
    logger.info("Upload logfile to lakeFS...")
    logger.info(f"Uploading {uci_dump_file_and_path} to lakeFS ({lakefs_repo} -> main -> {lakefs_path})")
    upload_and_commit_to_lakefs(
        path_and_file=logfile_name,
        lakefs_url=lakefs_url,
        lakefs_repo=lakefs_repo,
        lakefs_path=lakefs_path,
        msg="Logfile from current run",
        lakefs_user=creds["user"],
        lakefs_pwd=creds["password"],
    )
