import json
from pathlib import Path
from typing import List

from prefect import task, get_run_logger

from tasks.ucimlrepo_crawl import crawl_item


@task
async def update_dump(uci_dump_file_and_path: str, uci_dataset_ids: List[int] ) -> bool:
    """Checks whether all given dataset IDs are present in a JSON file.

    Args:
        uci_dump_file_and_path (str): Path to the JSON file containing dataset metadata.
        uci_dataset_ids (List[int]): List of dataset IDs to check.

    Returns:
        bool: True if entries have been updated, False otherwise.
    """
    logger = get_run_logger()

    if not Path(uci_dump_file_and_path).is_file():
        raise FileNotFoundError(f"JSON file not found: {uci_dump_file_and_path}")

    with open(uci_dump_file_and_path, "r", encoding="utf-8") as f:
        dump_data = json.load(f)

    available_ids = {entry["dataset_id"] for entry in dump_data}
    missing = [i for i in uci_dataset_ids if i not in available_ids]

    # If no new dataset has been found: return "False"
    if not missing:
        return False

    logger.info(f"Missing dataset_id(s): {missing}")

    updated_items = []

    for dataset_id in missing:
        # Crawl entry
        item_metadata = await crawl_item(dataset_id)
        updated_items.append(item_metadata)

    # extend it
    # Extend the existing dump data with the newly crawled items
    dump_data.extend(updated_items)

    # Write the updated dump data back to the file
    logger.info("Writing updated dump file...")
    with open(uci_dump_file_and_path, "w", encoding="utf-8") as f:
        json.dump(dump_data, f, indent=2, ensure_ascii=False)
    get_run_logger().info(f"Wrote updated dump to {uci_dump_file_and_path} ({len(dump_data)} datasets)")

    return True
