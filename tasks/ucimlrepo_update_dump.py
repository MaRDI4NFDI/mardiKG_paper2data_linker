import json
from pathlib import Path
from typing import List

from prefect import task


@task
def update_dump(uci_dump_file_and_path: str, uci_dataset_ids: List[int], ) -> bool:
    """Checks whether all given dataset IDs are present in a JSON file.

    Args:
        dataset_ids (List[int]): List of dataset IDs to check.
        json_path (str): Path to the JSON file containing dataset metadata.

    Returns:
        bool: True if all IDs are present, False otherwise.
    """
    if not Path(uci_dump_file_and_path).is_file():
        raise FileNotFoundError(f"JSON file not found: {uci_dump_file_and_path}")

    with open(uci_dump_file_and_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    available_ids = {entry["dataset_id"] for entry in data}
    missing = [i for i in uci_dataset_ids if i not in available_ids]

    if missing:
        print(f"Missing dataset_id(s): {missing}")
        return False

    return True