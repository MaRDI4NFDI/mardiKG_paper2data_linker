import json
import os
from pathlib import Path
from typing import Set, List
from prefect import task, get_run_logger
from mardiportal.workflowtools.mardikg_query import query_mardi_kg_for_arxivid


@task
def process_uci_ml_repo_dump(json_input: str) -> None:
    """This function loads paper metadata from a JSON file, searches a MediaWiki
    API for references to arXiv papers.

    Args:
        json_input (str): Path to the JSON input file.
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
    datasets_with_citations = _get_datasets_with_citations( data )
    datasets_to_process = _get_datasets_with_arxiv_citations( datasets_with_citations )
    logger.info("Collected %d datasets to check", len(datasets_to_process))

    # Check for entries for which the arXiv IDs are in the Knowledge Graph
    datasets_with_kg_entry = _check_arxiv_ids_in_kg( datasets_to_process, json_input+".kg_entries" )
    logger.info("Collected %d datasets to process", len(datasets_with_kg_entry))

    logger.info("Done.")

    return datasets_with_kg_entry


def _get_datasets_with_citations( data: list[dict] ) -> list[str]:
    """Filter input list to datasets that have at least one citation
    with arxiv/url/doi.

    Args:
        data (list[dict]): Full JSON dataset list.

    Returns:
        list[dict]: Filtered dataset list.
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


def _check_arxiv_ids_in_kg(datasets_with_arxiv: List[dict], cache_path: str) -> List[dict]:
    """Checks which arXiv IDs are found in the MaRDI Knowledge Graph.

    Args:
        datasets_with_arxiv (List[dict]): Filtered datasets with citations containing arXiv IDs.

    Returns:
        List[dict]: List of dictionaries with arXiv ID and matching MaRDI KG results.
    """
    logger = get_run_logger()

    # Check for results from previous run
    if os.path.exists(cache_path):
        logger.warning(f"Using cache: {cache_path}")
        with open(cache_path, "r", encoding="utf-8") as f:
            results = json.load(f)
            return results

    # Create new...
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
                    found_results.append({
                        "dataset_id": entry["dataset_id"],
                        "dataset_name": entry["dataset_name"],
                        "dataset_url": entry["dataset_url"],
                        "arxiv_id": arxiv_id,
                        "arxiv_title": citation["title"],
                        "QID": results[0]["qid"]
                    })
            except Exception as e:
                logger.warning(f"Query failed for arXiv:{arxiv_id} — {e}")

        # Save in cache
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(found_results, f, indent=2, ensure_ascii=False)

    return found_results


if __name__ == "__main__":
    process_uci_ml_repo_dump("../data/uci_datasets_final.json")
