import csv
import json
import os
from pathlib import Path
from typing import List, Dict
from prefect import task, get_run_logger

from mardiportal.workflowtools.mardikg_query import query_mardi_kg_for_arxivid

from tasks.ucimlrepo_kg_updates import link_publications_to_datasets_in_mardi_kg

@task
def link_papers_with_datasets(json_input: str, mapping_file: str) -> None:
    """
    Processes a UCI Machine Learning Repository metadata file and links arXiv-referenced
    publications to corresponding datasets in the MaRDI Knowledge Graph.

    The resulting list contains items describing a dataset from the UCI ML repo that
    has been cited in a publication and that publication is available in the MaRDI KG.

    Main workflow:
    1. Loads a JSON file containing UCI dataset metadata and associated citations.
    2. Filters out unapproved datasets and those without valid citations (arXiv, DOI, or URL).
    3. Further filters to datasets with at least one arXiv citation.
    4. For each arXiv ID, queries the MaRDI KG to check if the publication exists.
    5. Enriches matched publications with the QID of the cited dataset (if found via mapping).
    6. Creates `P223` (cites work) links between the publication and dataset items in the KG,
       with `P1689` (extracted from) references to the UCI dataset URL.

    Args:
        json_input (str): Path to the JSON file containing UCI dataset metadata and citations.
        mapping_file (str): Path to the CSV mapping file with UCI dataset ID to MaRDI QID.

    Returns:
        None
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

    # Filter for datasets with arXiv citations
    datasets_with_citations = _remove_datasets_without_citations(data)
    datasets_with_arxiv_citations = _get_datasets_with_arxiv_citations( datasets_with_citations )
    logger.info("Collected %d datasets to check", len(datasets_with_arxiv_citations))

    # Check for entries for which the arXiv IDs are found in the MaRDI Knowledge Graph
    # and adds the QIDs for the found (arXiv) publications
    datasets_with_kg_entry = _get_datasets_available_in_kg(
        datasets_with_arxiv_citations, json_input + ".kg_entries")
    logger.info("Collected %d datasets to process", len(datasets_with_kg_entry))

    # Add dataset QIDs to entries if the datasets exist in the KG
    # Returns only entries for which a dataset QID exists
    logger.info("Linking to KG datasets...")
    hits = _get_dataset_qids_from_kg(entries=datasets_with_kg_entry, mapping_file=mapping_file)

    # Perform linking in KG
    link_publications_to_datasets_in_mardi_kg( hits )

    logger.info("Done.")


def _remove_datasets_without_citations(data: list[dict]) -> list[dict]:
    """Filters UCI dataset entries to those with valid citations.

    This function:
    - Removes unapproved datasets.
    - Keeps only datasets that have non-empty citation lists.
    - Retains citations that include at least one of: arXiv ID, URL, or DOI.

    Args:
        data (list[dict]): List of dataset metadata entries (from UCI JSON dump).

    Returns:
        list[dict]: Filtered dataset entries with cleaned citation lists.
    """
    logger = get_run_logger()

    # Step 1: Filter out unapproved datasets
    valid_entries = 0
    approved_datasets = []
    for entry in data:
        if entry.get("dataset_name") != "403 Dataset not approved.":
            approved_datasets.append(entry)
            valid_entries += 1

    logger.debug("Valid entries: %d", valid_entries)

    # Step 2: Filter datasets with any citations at all
    datasets_with_citations = []

    for entry in approved_datasets:
        citations = entry.get("citations", [])
        if citations:
            datasets_with_citations.append(entry)
        #else:
        #    print(f"No citations at all for dataset_id {entry['dataset_id']}")

    logger.debug("Entries w/ citations: %d", len(datasets_with_citations))

    # Step 3: Keep only citations with arxiv, url, or doi
    datasets_to_process = []

    for entry in datasets_with_citations:
        filtered_citations = [
            c for c in entry["citations"]
            if c.get("arxiv") or c.get("url") or c.get("doi")
        ]

        if filtered_citations:
            entry["citations"] = filtered_citations
            datasets_to_process.append(entry)
        else:
            print(f"No valid citation fields in dataset_id {entry['dataset_id']}")

    return datasets_to_process


def _get_datasets_with_arxiv_citations(data: list[dict]) -> list[dict]:
    """Filters datasets to only those with at least one citation containing an arXiv ID.

    Args:
        data (list[dict]): List of dataset entries, each with a list of citations.

    Returns:
        list[dict]: Filtered list of datasets with at least one arXiv citation.
    """
    datasets_with_arxiv = []

    for entry in data:
        has_arxiv = any(c.get("arxiv") for c in entry.get("citations", []))
        if has_arxiv:
            datasets_with_arxiv.append(entry)

    return datasets_with_arxiv


def _get_datasets_available_in_kg(datasets_with_arxiv: List[dict], cache_path: str) -> List[dict]:
    """Checks which arXiv IDs from the UCI citations are found in the MaRDI Knowledge Graph.

    For each arXiv ID in the dataset's citations, it queries the MaRDI KG using the MediaWiki API.
    Matching results are collected and enriched with the corresponding QID.
    Uses a local cache to avoid re-querying already-processed IDs.

    Args:
        datasets_with_arxiv (List[dict]): List of dataset entries with arXiv citations.
        cache_path (str): Path to a local JSON file for caching previous query results.

    Returns:
        List[dict]: Filtered and enriched list of entries, preserving original fields and
                    appending the MaRDI KG QID under 'QID' when a match is found.
    """
    logger = get_run_logger()

    # Check if cached results exist - if yes: return those
    if os.path.exists(cache_path):
        logger.warning(f"Using cache: {cache_path}")
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)

    found_results = []

    for dataset_entry in datasets_with_arxiv:
        for citation in dataset_entry.get("citations", []):
            arxiv_id = citation.get("arxiv")
            if not arxiv_id:
                continue

            try:
                logger.info(f"Checking dataset '{dataset_entry['dataset_name']}' and "
                            f"citation with arXiv ID '{arxiv_id}'")
                results = query_mardi_kg_for_arxivid(arxiv_id)
                if results:
                    logger.info(f"arXiv:{arxiv_id} found {len(results)} result(s) in MaRDI KG")

                    # Preserve original fields from entry except "citation" & "intro_paper"
                    # and add info about found citation
                    combined_entry = {
                        **{k: v for k, v in dataset_entry.items() if k not in ("citations", "intro_paper")},
                        "arxiv_title": citation.get("title"),
                        "arxiv_id": citation.get("arxiv"),
                        "publication_mardi_QID": results[0]["qid"]
                    }

                    found_results.append(combined_entry)
            except Exception as e:
                logger.warning(f"Query failed for arXiv:{arxiv_id} — {e}")

    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(found_results, f, indent=2, ensure_ascii=False)

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
    link_papers_with_datasets("../data/uci_datasets_final.json")
