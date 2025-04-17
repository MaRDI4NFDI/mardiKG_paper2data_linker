import json
import os
from pathlib import Path
from typing import Set, List
from prefect import task, get_run_logger
from mardiportal.workflowtools.mardikg_query import query_mardi_kg_for_arxivid


@task
def process_uci_ml_repo_dump(json_input: str) -> None:
    """Processes a UCI Machine Learning Repository JSON dump and checks arXiv citations
    in the MaRDI Knowledge Graph.

    The resulting list contains items describing a dataset from the UCI ML repo that
    has been cited in a publication and that publication is available in the MaRDI KG.

    Main steps:
    1. Loads a JSON file containing UCI dataset metadata and associated citations.
    2. Filters datasets to those with valid citation data.
    3. Further filters to datasets with at least one arXiv citation.
    4. Queries the MaRDI KG for each arXiv ID and collects matched entries.

    Args:
        json_input (str): Path to the input JSON file containing UCI dataset metadata.

    Returns:
        List[dict]: A list of entries with matched arXiv IDs and MaRDI QIDs, including:
            - 'dataset_id' (int)
            - 'dataset_name' (str)
            - 'dataset_url' (str)
            - 'arxiv_id' (str)
            - 'arxiv_title' (str)
            - 'QID' (str)
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

    logger.info("Done.")

    return datasets_with_kg_entry


def _remove_datasets_without_citations(data: list[dict]) -> list[str]:
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

    for entry in datasets_with_arxiv:
        for citation in entry.get("citations", []):
            arxiv_id = citation.get("arxiv")
            if not arxiv_id:
                continue

            try:
                logger.info("Checking arXiv ID: %s", arxiv_id)
                results = query_mardi_kg_for_arxivid(arxiv_id)
                if results:
                    logger.info(f"arXiv:{arxiv_id} found {len(results)} result(s) in MaRDI KG")

                    # Preserve all original fields from entry and citation
                    combined_entry = {
                        **entry,
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



if __name__ == "__main__":
    process_uci_ml_repo_dump("../data/uci_datasets_final.json")
