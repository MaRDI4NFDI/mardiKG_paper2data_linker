from typing import List, Dict, Tuple

from prefect import task, get_run_logger
import csv


@task
def get_kg_qids(entries: List[Dict], mapping_file: str) -> List[Dict]:
    """Returns a minimal list of dicts with arXiv ID, paper QID, and related dataset QID.

    Args:
        entries (List[Dict]): Entries from _check_arxiv_ids_in_kg.
        mapping_file (str): Path to CSV file mapping UCI dataset_id to MaRDI QID.

    Returns:
        List[Dict]: Each item contains 'arxiv_id', 'QID', and 'dataset_mardi_qid'.
    """
    logger = get_run_logger()
    id_to_qid = _load_dataset_qid_mapping(mapping_file)
    result = []

    for entry in entries:
        dataset_id = entry.get("dataset_id")
        mardi_qid = id_to_qid.get(dataset_id)

        if not mardi_qid:
            logger.warning(f"No MaRDI QID found for dataset_id {dataset_id}")
            continue  # skip if no match

        result.append({
            "QID": entry["QID"],
            "arxiv_id": entry["arxiv_id"],
            "arxiv_title": entry["arxiv_title"],
            "dataset_name": entry["dataset_name"],
            "dataset_mardi_qid": mardi_qid
        })

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


