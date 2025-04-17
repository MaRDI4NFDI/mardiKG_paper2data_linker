from typing import List, Dict, Tuple

from prefect import task, get_run_logger
import csv


@task
def get_dataset_qids_from_kg(entries: List[Dict], mapping_file: str) -> List[Dict]:
    """Enriches entries with the corresponding MaRDI dataset QID based on UCI dataset ID.

    Each entry is expected to contain metadata for a paper linked to a UCI dataset.
    Only entries that have a matching 'dataset_mardi_qid' in the mapping will be retained.

    Args:
        entries (List[Dict]): List of entries, each having the field 'dataset_id' (int)
        mapping_file (str): Path to a CSV file mapping 'uci_id' to 'mardi_qid'.

    Returns:
        List[Dict]: Filtered list of entries, each with an added 'dataset_mardi_qid'.
    """
    logger = get_run_logger()
    id_to_qid = _load_dataset_qid_mapping(mapping_file)
    result = []

    for entry in entries:
        dataset_id = entry.get("dataset_id")
        mardi_qid = id_to_qid.get(dataset_id)

        if mardi_qid:
            new_entry = dict(entry)
            new_entry["dataset_mardi_QID"] = mardi_qid
            result.append(new_entry)
        else:
            logger.warning(f"No MaRDI QID found for UCI dataset_id {dataset_id} — entry skipped.")

    return result



def _load_dataset_qid_mapping(mapping_file: str) -> Dict[int, str]:
    """Loads a mapping from UCI dataset ID to MaRDI QID from a CSV file.

    The CSV must have the following header:
    dataset_name,uci_id,mardi_qid,mardi_dataset_name

    Args:
        mapping_file (str): Path to the CSV mapping file.

    Returns:
        Dict[int, str]: Mapping from UCI dataset_id to MaRDI QID.
    """
    mapping = {}
    with open(mapping_file, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                uci_id = int(row["uci_id"])
                qid = row["mardi_qid"]
                mapping[uci_id] = qid
            except (ValueError, KeyError):
                continue  # Skip malformed rows
    return mapping


