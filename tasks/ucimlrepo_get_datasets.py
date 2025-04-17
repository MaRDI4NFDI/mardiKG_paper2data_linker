import json
import ssl
import urllib
from typing import Optional, List, Dict

import certifi
from prefect import task

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
    # Validate inputs
    if filter and not isinstance(filter, str):
        raise ValueError("Filter must be a string")
    if search and not isinstance(search, str):
        raise ValueError("Search query must be a string")
    if area and not isinstance(area, str):
        raise ValueError("Area must be a string")

    # Build query params
    query_params = {
        "filter": filter.lower() if filter else "python"
    }
    if search:
        query_params["search"] = search.lower()
    if area:
        query_params["area"] = area

    # Construct full URL
    api_list_url = API_LIST_URL + "?" + urllib.parse.urlencode(query_params)

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

    return data


if __name__ == "__main__":
    datasets = get_available_datasets(filter="python", search="iris")
    for ds in datasets:
        print(f"{ds['id']:>3} - {ds['name']}")

    datasets = get_available_datasets()
    for ds in datasets:
        print(f"{ds['id']:>3} - {ds['name']}")
