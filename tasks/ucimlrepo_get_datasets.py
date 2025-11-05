import logging
import json
import ssl
import urllib
from typing import Optional, List, Dict

import certifi
from prefect import task, get_run_logger
from prefect.exceptions import MissingContextError

API_BASE_URL = 'https://archive.ics.uci.edu/api/dataset'
API_LIST_URL = 'https://archive.ics.uci.edu/api/datasets/list'

@task
def get_available_datasets(
    filter: Optional[str] = None,
    search: Optional[str] = None,
    area: Optional[str] = None
) -> List[Dict]:
    """
    Retrieves a list of available datasets from the UCI repository API.

    Args:
        filter (str, optional): Filter datasets by label (e.g., 'python').
        search (str, optional): Search query for dataset names.
        area (str, optional): Filter datasets by subject area.

    Returns:
        List[Dict]: A list of available dataset metadata dictionaries.
    """
    logger = get_logger_safe()
    logger.info("Checking for UCI entries...")

    # Validate inputs
    if filter and not isinstance(filter, str):
        raise ValueError("Filter must be a string")
    if search and not isinstance(search, str):
        raise ValueError("Search query must be a string")
    if area and not isinstance(area, str):
        raise ValueError("Area must be a string")

    # Build query params
    query_params = {
        "filter": filter.lower() if filter else ""
    }
    if search:
        query_params["search"] = search.lower()
    if area:
        query_params["area"] = area

    # Construct full URL
    api_list_url = API_LIST_URL + "?" + urllib.parse.urlencode(query_params)
    print(api_list_url)

    logger.info(f"UCI API call: {api_list_url}")

    try:
        response = urllib.request.urlopen(
            api_list_url,
            context=ssl.create_default_context(cafile=certifi.where())
        )
        resp_json = json.load(response)
    except (urllib.error.URLError, urllib.error.HTTPError):
        raise ConnectionError("Error connecting to server")

    if resp_json.get("status") != 200:
        error_msg = resp_json.get("message", "Internal Server Error")
        raise ValueError(error_msg)

    data = resp_json.get("data", [])
    data_ids = [d["id"] for d in data]

    logger.info(f"Found {len(data_ids)} datasets in result.")

    return data


def get_logger_safe():
    """
    Returns a logger either from Prefect or a standard logger, if run locally.
    """
    try:
        return get_run_logger()
    except MissingContextError:
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger("local")


if __name__ == "__main__":
    datasets = get_available_datasets.fn(filter="python", search="iris")
    for ds in datasets:
        print(f"{ds['id']:>3} - {ds['name']}")

    datasets = get_available_datasets.fn()
    for ds in datasets:
        print(f"{ds['id']:>3} - {ds['name']}")
