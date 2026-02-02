"""report.py - Walk mzid_store and generate analysis report CSV."""

import csv
import logging
import os
from datetime import date

from lxml import etree

# Logging setup
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
logger = logging.getLogger(__name__)

# Config
MZID_STORE = os.path.expanduser("~") + "/mzid_store"
REPORT_PATH = MZID_STORE + "/report.csv"
CROSSLINKING_PATH = MZID_STORE + "/all_crosslinking.csv"


def format_file_size(size_bytes: int) -> str:
    """Format file size in human readable format."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.1f} {unit}" if unit != "B" else f"{size_bytes} B"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


CSV_FIELDNAMES = [
    "project",
    "date",
    "file_name",
    "file_size",
    "contains_MS1002511",
    "parseable",
    "schema_version",
    "schema_valid",
    "error_message",
]


def parse_directory(dirpath: str) -> tuple[str, date | None]:
    """Parse directory path to extract project name and date.

    Expected format: .../YY/MM/project_name
    Returns: (project_name, date) or (project_name, None) if parsing fails
    """
    parts = dirpath.rstrip("/").split("/")
    if len(parts) >= 3:
        project = parts[-1]
        try:
            month = int(parts[-2])
            year = int(parts[-3])
            # Assume 2000s for 2-digit years
            if year < 100:
                year += 2000
            return project, date(year, month, 1)
        except (ValueError, IndexError):
            return parts[-1], None
    return dirpath, None


def is_archive(filename: str) -> bool:
    """Check if filename is a compressed archive."""
    lower = filename.lower()
    return lower.endswith(".zip") or lower.endswith(".gz") or lower.endswith(".gzip")


def contains_string(file_path: str, search_string: str, chunk_size: int = 1024 * 1024) -> bool:
    """Check if file contains a string (chunked binary search for memory safety)."""
    try:
        search_bytes = search_string.encode("utf-8")
        overlap = len(search_bytes) - 1
        with open(file_path, "rb") as f:
            prev_tail = b""
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                # Search in overlap + current chunk to catch matches spanning chunks
                data = prev_tail + chunk
                if search_bytes in data:
                    return True
                prev_tail = chunk[-overlap:] if len(chunk) > overlap else chunk
        return False
    except Exception as e:
        logger.warning(f"Error reading {file_path} for string search: {e}")
        return False


def check_parseable(file_path: str) -> tuple[bool, str | None]:
    """Try to parse file with lxml iterparse (memory-efficient).

    Returns:
        (True, None) if parseable, (False, error_message) if not
    """
    try:
        # Use iterparse to avoid loading entire tree into memory
        for event, elem in etree.iterparse(file_path, events=("end",)):
            elem.clear()  # Free memory as we go
        return True, None
    except Exception as e:
        return False, str(e)


def extract_schema_version(file_path: str) -> str | None:
    """Extract schema version from mzIdentML file.

    Reads only the root element to get the schemaLocation attribute or version attribute.
    Returns version string like '1.2.0' or None if not found.
    """
    try:
        # Use iterparse and stop after the first start event (root element)
        for event, elem in etree.iterparse(file_path, events=("start",)):
            # Try schemaLocation first
            schema_location = elem.attrib.get(
                "{http://www.w3.org/2001/XMLSchema-instance}schemaLocation"
            )
            if not schema_location:
                schema_location = elem.attrib.get(
                    "{http://www.w3.org/2001/XMLSchema-instance}noNamespaceSchemaLocation"
                )

            if schema_location:
                # Extract schema filename from URL
                schema_parts = schema_location.split()
                if len(schema_parts) >= 2:
                    schema_url = schema_parts[-1]
                    schema_fname = schema_url.split("/")[-1]
                    # Extract version from filename like "mzIdentML1.2.0.xsd"
                    if schema_fname.startswith("mzIdentML") and schema_fname.endswith(".xsd"):
                        return schema_fname[9:-4]  # Remove prefix and suffix

            # Fallback: check version attribute on root element
            version = elem.attrib.get("version")
            if version:
                return version

            break  # Only process root element
        return None
    except Exception:
        return None


def load_existing_report() -> set[tuple[str, str]]:
    """Load existing report and return set of (project, file_name) tuples."""
    existing = set()
    if os.path.exists(REPORT_PATH):
        try:
            with open(REPORT_PATH, "r", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing.add((row["project"], row["file_name"]))
            logger.info(f"Loaded {len(existing)} existing entries from report")
        except Exception as e:
            logger.warning(f"Error loading existing report: {e}")
    return existing


def generate_report():
    """Walk mzid_store and generate/append to report CSV."""
    existing_entries = load_existing_report()
    file_exists = os.path.exists(REPORT_PATH)

    with open(REPORT_PATH, "a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=CSV_FIELDNAMES)

        # Write header only if new file
        if not file_exists:
            writer.writeheader()

        for dirpath, dirnames, filenames in os.walk(MZID_STORE):
            for filename in filenames:
                # Skip the report file itself
                if filename == "report.csv":
                    continue

                # Skip archives
                if is_archive(filename):
                    logger.debug(f"Skipping archive: {filename}")
                    continue

                # Parse directory for project and date
                project, file_date = parse_directory(dirpath)

                # Skip already processed files
                if (project, filename) in existing_entries:
                    logger.debug(f"Skipping already processed: {project}/{filename}")
                    continue

                file_path = os.path.join(dirpath, filename)
                logger.info(f"Processing: {file_path}")

                # Get file size
                file_size = os.path.getsize(file_path)

                # Check for MS:1002511
                has_ms1002511 = contains_string(file_path, "MS:1002511")

                # Check if parseable
                parseable, parse_error = check_parseable(file_path)

                # Extract schema version
                schema_version = extract_schema_version(file_path) if parseable else None

                # Set error message if not parseable
                error_message = parse_error if not parseable else None

                # Write row
                writer.writerow(
                    {
                        "project": project,
                        "date": file_date.isoformat() if file_date else "",
                        "file_name": filename,
                        "file_size": format_file_size(file_size),
                        "contains_MS1002511": has_ms1002511,
                        "parseable": parseable,
                        "schema_version": schema_version or "",
                        "schema_valid": "",
                        "error_message": error_message or "",
                    }
                )
                csvfile.flush()  # Flush after each row for incremental progress

    logger.info(f"Report written to {REPORT_PATH}")


def generate_crosslinking_report():
    """Create all_crosslinking.csv with rows where contains_MS1002511 is True."""
    if not os.path.exists(REPORT_PATH):
        logger.warning("Report not found, cannot generate crosslinking report")
        return

    # Delete existing crosslinking report if it exists
    if os.path.exists(CROSSLINKING_PATH):
        os.remove(CROSSLINKING_PATH)
        logger.info(f"Deleted existing {CROSSLINKING_PATH}")

    count = 0
    with open(REPORT_PATH, "r", newline="") as infile, \
         open(CROSSLINKING_PATH, "w", newline="") as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()

        for row in reader:
            if row["contains_MS1002511"] == "True":
                writer.writerow(row)
                count += 1

    logger.info(f"Crosslinking report written to {CROSSLINKING_PATH} ({count} rows)")


if __name__ == "__main__":
    generate_report()
    generate_crosslinking_report()
