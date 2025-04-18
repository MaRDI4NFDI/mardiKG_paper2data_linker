import csv
import json
from pathlib import Path
from typing import List, Dict

from mardiportal.workflowtools import query_mardi_kg_for_arxivid
from prefect import task, get_run_logger

from tasks.ucimlrepo_kg_updates import link_publications_to_datasets_in_mardi_kg


@task
def link_intropapers_with_datasets(json_input: str, mapping_file: str) -> List[Dict]:
    """
    Processes UCI datasets to link their intro papers to entries in the MaRDI Knowledge Graph (KG).

    This function:
    1. Loads UCI dataset metadata from a JSON file.
    2. Filters for datasets that include an `intro_paper` with a non-null DOI or arXiv ID.
    3. Checks whether the arXiv-referenced intro papers are present in the MaRDI KG.
    4. Adds the MaRDI QID of the publication to the intro paper if found.
    5. Adds the MaRDI QID of the dataset based on a provided mapping file.

    Args:
        json_input (str): Path to the input JSON file containing UCI dataset metadata.
        mapping_file (str): Path to the CSV mapping file containing UCI dataset ID to MaRDI QID.

    Returns: List
    """

    # Set logging
    logger = get_run_logger()

    # Load papers from JSON dump
    logger.info("Loading papers from %s …", json_input)
    if not Path(json_input).exists():
        logger.error("Required file does not exist")
        raise FileNotFoundError(f"Input file not found: {json_input}")
    with open(json_input, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Step 1: Filter to entries with intro_paper and either DOI or arXiv
    datasets_with_valid_intro_papers: List[Dict] = [
        entry for entry in data
        if entry.get("intro_paper") and (
                entry["intro_paper"].get("DOI") or entry["intro_paper"].get("arxiv")
        )
    ]

    logger.info("Found %d datasets with intro papers having DOI or arXiv.",
                len(datasets_with_valid_intro_papers))

    # Step 2: check which ones are available in the KG
    intropapers_with_kg_entry = _get_intro_papers_available_in_kg(datasets_with_valid_intro_papers)

    logger.info("Found %d datasets with intro papers available in KG.",
                len(intropapers_with_kg_entry))

    if not intropapers_with_kg_entry:
        return []

    # Step 3:Add dataset QIDs to entries if the datasets exist in the KG
    # Returns only entries for which a dataset QID exists
    logger.info("Get dataset ID mapping...")
    hits = _get_dataset_qids_from_kg(entries=intropapers_with_kg_entry, mapping_file=mapping_file)

    # Perform linking in KG
    logger.info("Linking to KG datasets...")

    # Convert hits into structure expected by the linker
    converted_hits = [
        {
            "publication_mardi_QID": entry["intro_paper"]["publication_mardi_QID"],
            "dataset_mardi_QID": entry["dataset_mardi_QID"],
            "dataset_url": entry["dataset_url"]
        }
        for entry in hits
        if entry.get("intro_paper", {}).get("publication_mardi_QID") and entry.get("dataset_mardi_QID")
    ]

    link_publications_to_datasets_in_mardi_kg( converted_hits )

    return hits


def _get_intro_papers_available_in_kg(datasets_with_intro_papers: List[Dict]) -> List[Dict]:
    """
    For each dataset with an intro paper (containing an arXiv ID), checks if that arXiv ID
    is present in the MaRDI Knowledge Graph.

    Args:
        datasets_with_intro_papers (List[Dict]): List of UCI dataset entries whose intro_paper contains
                                                 a DOI or arXiv ID.

    Returns:
        List[Dict]: Entries enriched with matching publication QID from the KG (if arXiv found).
                    Includes fields: "dataset_id" and "intro_paper" including "publication_mardi_QID"
    """
    logger = get_run_logger()
    found_results = []

    for entry in datasets_with_intro_papers:
        intro_paper = entry.get("intro_paper", {})
        arxiv_id = intro_paper.get("arxiv")

        if not arxiv_id:
            continue

        try:
            logger.info(f"Querying KG for arXiv ID: {arxiv_id}")
            results = query_mardi_kg_for_arxivid(arxiv_id)
            if results:
                logger.info(f"Found {len(results)} result(s) for arXiv:{arxiv_id} in KG")

                intro_paper["publication_mardi_QID"] = results[0]["qid"]

                combined_entry = {
                    "dataset_id": entry["dataset_id"],
                    "dataset_name": entry["dataset_name"],
                    "dataset_url": entry["dataset_url"],
                    "intro_paper": intro_paper
                }

                found_results.append(combined_entry)

        except Exception as e:
            logger.warning(f"Query failed for arXiv ID {arxiv_id}: {e}")

    return found_results


def _get_dataset_qids_from_kg(entries: List[Dict], mapping_file: str) -> List[Dict]:
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

if __name__ == "__main__":
    datasets = link_intropapers_with_datasets(
        json_input="../data/uci_datasets_final.json",
        mapping_file="../data/uci2mardi_dataset_mapping.txt")

    for d in datasets:
        print(d["dataset_id"], d["intro_paper"]["arxiv"])
