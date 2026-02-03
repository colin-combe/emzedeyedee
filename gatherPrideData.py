"""gatherPrideData.py - Fetch PRIDE metadata for all projects in mzid_store."""

import json
import logging
import os
import time
import urllib.request
import urllib.error

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
logger = logging.getLogger(__name__)

MZID_STORE = os.path.expanduser("~") + "/mzid_store"
PRIDE_API_BASE = "https://www.ebi.ac.uk/pride/ws/archive/v2/projects/"
METADATA_FILENAME = "pride_metadata.json"


def fetch_pride_metadata(pxd: str, max_retries: int = 5, base_delay: float = 1.0) -> dict | None:
    """Fetch project metadata from PRIDE API with exponential backoff.

    Args:
        pxd: Project accession (e.g., 'PXD000001')
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay in seconds before first retry

    Returns:
        Parsed JSON metadata dict, or None if fetch failed
    """
    url = PRIDE_API_BASE + pxd
    delay = base_delay

    for attempt in range(1, max_retries + 1):
        try:
            time.sleep(1)  # Rate limiting
            with urllib.request.urlopen(url, timeout=30) as response:
                data = response.read().decode("utf-8")
                return json.loads(data)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                logger.warning(f"{pxd}: Project not found in PRIDE API (404)")
                return None
            logger.error(f"{pxd}: HTTP error {e.code} on attempt {attempt}")
        except urllib.error.URLError as e:
            logger.error(f"{pxd}: URL error on attempt {attempt}: {e.reason}")
        except Exception as e:
            logger.error(f"{pxd}: Error on attempt {attempt}: {e}")

        if attempt < max_retries:
            logger.info(f"{pxd}: Retrying in {delay:.1f}s...")
            time.sleep(delay)
            delay *= 2  # Exponential backoff

    logger.error(f"{pxd}: Failed after {max_retries} attempts")
    return None


def gather_all_metadata():
    """Walk mzid_store and fetch metadata for all projects."""
    projects_found = 0
    projects_fetched = 0
    projects_skipped = 0
    projects_failed = 0

    for dirpath, dirnames, filenames in os.walk(MZID_STORE):
        dirname = os.path.basename(dirpath)

        # Check if this is a PXD project directory
        if not dirname.startswith("PXD"):
            continue

        projects_found += 1
        pxd = dirname
        metadata_path = os.path.join(dirpath, METADATA_FILENAME)

        # Skip if metadata already exists
        if os.path.exists(metadata_path):
            logger.debug(f"{pxd}: Metadata already exists, skipping")
            projects_skipped += 1
            continue

        logger.info(f"{pxd}: Fetching metadata...")
        metadata = fetch_pride_metadata(pxd)

        if metadata:
            with open(metadata_path, "w") as f:
                json.dump(metadata, f, indent=2)
            logger.info(f"{pxd}: Saved metadata to {metadata_path}")
            projects_fetched += 1
        else:
            projects_failed += 1

    logger.info(
        f"Complete: {projects_found} projects found, "
        f"{projects_fetched} fetched, {projects_skipped} skipped, {projects_failed} failed"
    )


if __name__ == "__main__":
    gather_all_metadata()
