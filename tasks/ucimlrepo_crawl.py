# Make sure to initialize crawl4ai:
# -> crawl4ai-setup

import asyncio
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict

import httpx
import requests
import json
import os

from prefect import task, get_run_logger
from selectolax.parser import HTMLParser

@task
async def start_ucimlrepo_full_crawl(uci_dump_file: str, dataset_id_list: List[int]):
    """Main asynchronous entry point for processing UCI datasets.

    Iterates through datasets, enriches metadata, and saves progress incrementally.
    """
    progress_suffix = '_progress'
    logger = get_run_logger()
    logger.info("Starting crawl...")

    if Path(uci_dump_file + progress_suffix).is_file():
        results, start_id = _load_progress(uci_dump_file + progress_suffix)
        logger.info(f"Resuming from dataset ID {start_id}")
    else:
        results = []

    for dataset_id in dataset_id_list:
        try:
            result = await crawl_item(dataset_id)
            results.append(result)

            if dataset_id % 10 == 0:
                _save_progress(results, uci_dump_file + progress_suffix)

        except Exception as e:
            logger.warning(f"Error processing dataset {dataset_id}: {e}")

    _save_progress(results, uci_dump_file)


async def crawl_item(dataset_id: int) -> Dict:
    """
    Crawls metadata for a single UCI dataset including intro paper, citations, and basic info.

    Args:
        dataset_id (int): The UCI dataset ID to crawl.

    Returns:
        Dict: A dictionary containing:
              dataset_id, dataset_name, dataset_url, intro_paper, citations and timestamps.
    """

    # Get logger
    logger = None
    try:
        logger = get_run_logger()
    except RuntimeError:
        logger = logging.getLogger(__name__)

    logger.info(f"Processing dataset ID {dataset_id}")

    # Get intro paper from UCI API
    intro_paper = _get_dataset_intro_paper(dataset_id)

    # Get detailed metadata from HTML page
    metadata_md = await _get_dataset_metadata_as_md(dataset_id)
    dataset_name = _get_name_from_metadata_md(metadata_md)
    dataset_url = _get_url_from_metadata_md(metadata_md)
    citations = _get_citations_from_metadata_md(metadata_md)

    enriched_citations = []
    for citation in citations:
        paper_id = _extract_corpus_id(citation["url"])
        if not paper_id:
            continue

        api_url = (
            f"https://api.semanticscholar.org/graph/v1/paper/{paper_id}"
            f"?fields=title,url,externalIds"
        )
        response = requests.get(api_url)
        if response.status_code == 200:
            paper_info = response.json()
            enriched_citations.append({
                "title": paper_info.get("title"),
                "url": paper_info.get("url"),
                "doi": paper_info.get("externalIds", {}).get("DOI"),
                "arxiv": paper_info.get("externalIds", {}).get("ArXiv")
            })

    # Get current UTC timestamp in ISO 8601 format
    timestamp = datetime.now(timezone.utc).replace(second=0, microsecond=0).isoformat().replace("+00:00", "Z")

    return {
        "dataset_id": dataset_id,
        "checked_timestamp": timestamp,
        "updated_timestamp": timestamp,
        "dataset_name": dataset_name,
        "dataset_url": dataset_url,
        "intro_paper": intro_paper,
        "citations": enriched_citations
    }

def _extract_citations(text):
    """Extracts citation links from the markdown metadata.

    Looks for a section between "# Papers Citing this Dataset" and "# Reviews",
    and extracts citations formatted as markdown links.

    Args:
        text (str): The markdown text to search.

    Returns:
        list[dict]: A list of dictionaries with 'title' and 'url' keys.
    """
    match = re.search(r"# Papers Citing this Dataset(.*?)(?:\n# |\Z)", text, re.DOTALL)
    if not match:
        return []

    section = match.group(1)
    pattern = re.compile(r'^\[\s*(.*?)\s*\]\(.*?<\s*(.*?)\s*>\)', re.MULTILINE)
    results = []

    for title, url in pattern.findall(section):
        results.append({'title': title, 'url': url})

    return results


async def _get_dataset_metadata_as_md(dataset_id: int) -> str:
    """Fetches dataset metadata from UCI ML Repo and formats it as markdown."""
    url = f"https://archive.ics.uci.edu/dataset/{dataset_id}/"

    async with httpx.AsyncClient(follow_redirects=True) as client:
        response = await client.get(url)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch dataset page: {response.status_code}")
        html_text = response.text
        html = HTMLParser(html_text)

    content = []

    # --- Dataset Name from <title> tag ---
    title_match = re.search(r"<title>(.*?)</title>", html_text)
    if title_match:
        name = title_match.group(1).replace(" - UCI Machine Learning Repository", "").strip()
        content.append(f"# {name}")
    else:
        content.append("# Unknown Dataset")

    # --- Dataset URL ---
    content.append(f"[](https://archive.ics.uci.edu/dataset/{dataset_id}/<UCI Dataset Page>)")

    # --- DOI (needed for URL) ---
    doi_match = re.search(r'https://doi\.org/10\.\d{4,9}/[-._;()/:A-Z0-9]+', html_text, re.IGNORECASE)
    if doi_match:
        doi_url = doi_match.group(0).rstrip(".")
        content.append("## DOI")
        content.append(f"[{doi_url}]({doi_url})")

    # --- BibTeX Citation ---
    bibtex_match = re.search(r'"BibTeX":"(.*?)"}', html_text)
    if bibtex_match:
        bibtex_raw = bibtex_match.group(1)
        bibtex = bibtex_raw.encode("utf-8").decode("unicode_escape")
        content.append("## BibTeX Citation")
        content.append(f"```bibtex\n{bibtex}\n```")

    # --- File list ---
    files_match = re.search(r'"headers":\["File","Size"\],"data":(\[\[.*?\]\])', html_text)
    if files_match:
        try:
            files_json = json.loads(files_match.group(1))
            content.append("## Files")
            for name, size in files_json:
                content.append(f"- **{name}** ({size})")
        except Exception:
            pass

    # --- Creators ---
    creators = [p.text(strip=True) for p in html.css("p.font-semibold") if p.parent and "Creators" in p.parent.parent.text()]
    if creators:
        content.append("## Creators")
        for creator in creators:
            content.append(f"- {creator}")

    # --- Citing Papers (for downstream _extract_citations) ---
    paper_blocks = html.css("a.text-primary.underline")
    citing_papers = []
    for block in paper_blocks:
        title = block.text(strip=True)
        url = block.attributes.get("href")
        if title and url and url.startswith("http"):
            citing_papers.append((title, url))

    if citing_papers:
        content.append("# Papers Citing this Dataset")
        for title, url in citing_papers:
            content.append(f"[{title}](<{url}>)")

    return "\n\n".join(content)



def _extract_corpus_id(url):
    """Extracts the Semantic Scholar Corpus ID from a URL.

    Args:
        url (str): The URL containing the Corpus ID.

    Returns:
        str or None: The Corpus ID if found, otherwise None.
    """
    match = re.search(r'CorpusID:(\d+)', url)
    return f"CorpusID:{match.group(1)}" if match else None


def _get_citations_from_metadata_md(metadata_md):
    """Parses markdown metadata and returns citation entries.

    Args:
        metadata_md (str): The markdown metadata content.

    Returns:
        list[dict]: A list of citations with titles and URLs.
    """
    return _extract_citations(metadata_md)


def _get_url_from_metadata_md(text):
    """Extracts the dataset URL from markdown metadata.

    Args:
        text (str): The markdown content.

    Returns:
        str or None: The dataset URL, or None if not found.
    """
    match = re.search(r'\[\]\((https?://archive\.ics\.uci\.edu/dataset/\d+)/<.*?>\)', text)
    return match.group(1) if match else None


def _get_name_from_metadata_md(text):
    """Extracts the dataset name from the markdown content.

    Looks for the first level-1 heading (line starting with '# ').

    Args:
        text (str): The markdown content.

    Returns:
        str or None: The dataset name, or None if not found.
    """
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("# "):
            return line[2:].strip()
    return None


def _get_dataset_intro_paper(dataset_id: int):
    """Fetches the introductory paper metadata for a given dataset from the UCI API.

    Args:
        dataset_id (int): The ID of the dataset.

    Returns:
        dict or None: A dictionary with intro paper info or None if not available.
    """
    api_url = f"https://archive.ics.uci.edu/api/dataset?id={dataset_id}"
    try:
        response = requests.get(api_url)
        if response.status_code == 200:
            data = response.json()
            intro_paper = data.get("data", {}).get("intro_paper")

            if not intro_paper:
                return None

            return {
                "title": intro_paper.get("title"),
                "DOI": intro_paper.get("DOI"),
                "URL": intro_paper.get("URL"),
                "corpus": intro_paper.get("corpus"),
                "arxiv": intro_paper.get("arxiv")
            }
        else:
            print(f"API request failed for dataset {dataset_id}: {response.status_code}")
    except Exception as e:
        print(f"Error fetching intro paper for dataset {dataset_id}: {e}")
    return None


def _save_progress(results: list, path: str):
    """Saves the current progress to a JSON file.

    Args:
        results (list): The list of collected dataset results.
        path (str, optional): File path to save progress. Defaults to "uci_datasets_progress.json".
    """
    with open(path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    get_run_logger().info(f"Progress saved to {path} ({len(results)} datasets)")


def _load_progress(path):
    """Loads progress from a previously saved JSON file.

    Args:
        path (str, optional): The path to the saved JSON file. Defaults to "uci_datasets_progress.json".

    Returns:
        tuple[list, int]: A tuple containing the results list and the next dataset ID to process.
    """
    if not os.path.exists(path):
        return [], 1

    with open(path, "r", encoding="utf-8") as f:
        results = json.load(f)
        if results:
            last_id = results[-1]["dataset_id"]
            return results, last_id + 1
        else:
            return [], 0


if __name__ == "__main__":
    # asyncio.run(start_ucimlrepo_full_crawl("../data/uci_datasets_progress.json"))

    result = asyncio.run( crawl_item(5) )
    print( result )

    """
    metadata_md = asyncio.run(_get_dataset_metadata_as_md(5))
    dataset_name = _get_name_from_metadata_md(metadata_md)
    dataset_url = _get_url_from_metadata_md(metadata_md)
    citations = _get_citations_from_metadata_md(metadata_md)

    print( f"result: {metadata_md}")

    print( f"ds name: {dataset_name}" )
    print( f"ds url : {dataset_url}")
    print( "citations" )
    print( citations )
    """
