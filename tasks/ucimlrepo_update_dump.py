import json
from pathlib import Path
from typing import List

from prefect import task

from tasks.ucimlrepo_crawl import crawl_item


@task
async def update_dump(uci_dump_file_and_path: str, uci_dataset_ids: List[int], ) -> bool:
    """Checks whether all given dataset IDs are present in a JSON file.

    Args:
        dataset_ids (List[int]): List of dataset IDs to check.
        json_path (str): Path to the JSON file containing dataset metadata.

    Returns:
        bool: True, if changes have been detected.
    """
    if not Path(uci_dump_file_and_path).is_file():
        raise FileNotFoundError(f"JSON file not found: {uci_dump_file_and_path}")

    with open(uci_dump_file_and_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    available_ids = {entry["dataset_id"] for entry in data}
    missing = [i for i in uci_dataset_ids if i not in available_ids]

    # If no new dataset has been found: return
    if not missing:
        return False

    print(f"Missing dataset_id(s): {missing}")

    # TODO: add logic for inserting into JSON dump
    for dataset_id in missing:
        # Crawl entry
        item_metadata = await crawl_item(dataset_id)

        print( item_metadata )

    return True
