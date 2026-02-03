"""validate_schemas.py - Validate schemas for mzIdentML files."""

import csv
import logging
import os
from multiprocessing import TimeoutError as MpTimeoutError

from schema_validate import schema_validate_with_messages

# Logging setup
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
logger = logging.getLogger(__name__)

# Config
MZID_STORE = os.path.expanduser("~") + "/mzid_store"
REPORT_PATH = MZID_STORE + "/report.csv"
CROSSLINKING_PATH = MZID_STORE + "/all_crosslinking.csv"
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


def parse_file_size(size_str: str) -> int:
    """Parse human-readable file size back to bytes."""
    size_str = size_str.strip()
    # Check longer units first to avoid "MB" matching "B"
    units = [("PB", 1024**5), ("TB", 1024**4), ("GB", 1024**3), ("MB", 1024**2), ("KB", 1024), ("B", 1)]
    for unit, multiplier in units:
        if size_str.endswith(unit):
            number = size_str[: -len(unit)].strip()
            return int(float(number) * multiplier)
    # Fallback: try parsing as raw number
    return int(float(size_str))


def find_file_path(project: str, filename: str) -> str | None:
    """Find full file path given project and filename."""
    for dirpath, dirnames, filenames in os.walk(MZID_STORE):
        if filename in filenames and dirpath.endswith(project):
            return os.path.join(dirpath, filename)
    return None


def validate_schemas(csv_path: str, label: str, filter_func=None):
    """Validate schemas for mzid files listed in a CSV, smallest to largest.

    Args:
        csv_path: Path to the CSV file to read/update
        label: Label for logging (e.g., "crosslinking", "non-crosslinking")
        filter_func: Optional function to filter rows (returns True to include)
    """
    if not os.path.exists(csv_path):
        logger.warning(f"{label} report not found: {csv_path}")
        return

    # Read all rows and sort by file size
    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    rows.sort(key=lambda r: parse_file_size(r["file_size"]))

    # Filter rows if filter function provided
    if filter_func:
        rows_to_validate = [r for r in rows if filter_func(r)]
    else:
        rows_to_validate = rows

    logger.info(f"Validating schemas for {len(rows_to_validate)} {label} files")

    # Process each row and update schema_valid/error_message
    for i, row in enumerate(rows_to_validate):
        if row["parseable"] != "True":
            continue  # Skip unparseable files

        # Skip already validated files
        if row["schema_valid"] in ("True", "False"):
            logger.debug(f"Skipping already validated: {row['file_name']}")
            continue

        file_path = find_file_path(row["project"], row["file_name"])
        if not file_path:
            logger.warning(f"Could not find file: {row['project']}/{row['file_name']}")
            continue

        logger.info(f"[{i+1}/{len(rows_to_validate)}] Validating schema: {file_path}")

        try:
            schema_valid, schema_version, messages = schema_validate_with_messages(file_path)
            row["schema_version"] = schema_version or ""
            # Check if validation timed out
            if messages and "timed out" in messages[0].lower():
                row["schema_valid"] = "timed out"
                row["error_message"] = messages[0]
            elif schema_valid:
                row["schema_valid"] = True
                row["error_message"] = ""
            else:
                row["schema_valid"] = False
                row["error_message"] = "; ".join(messages) if messages else ""
        except MemoryError as e:
            row["schema_valid"] = "out of memory"
            row["error_message"] = f"Memory error: {e}"
            logger.error(f"Memory error validating {file_path}. Stopping - larger files will also fail.")
            # Write final state before stopping
            with open(csv_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
                writer.writeheader()
                writer.writerows(rows)
            return  # Stop processing
        except (EOFError, BrokenPipeError, ConnectionResetError) as e:
            # Subprocess was likely killed (OOM or other)
            row["schema_valid"] = "out of memory"
            row["error_message"] = f"Subprocess killed: {e}"
            logger.error(f"Subprocess killed validating {file_path}. Stopping - larger files will also fail.")
            # Write final state before stopping
            with open(csv_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
                writer.writeheader()
                writer.writerows(rows)
            return  # Stop processing
        except Exception as e:
            error_str = str(e).lower()
            error_type = type(e).__name__.lower()
            # Detect memory-related subprocess failures
            if any(term in error_str + error_type for term in ["kill", "memory", "oom", "signal 9", "cannot allocate", "worker"]):
                row["schema_valid"] = "out of memory"
                row["error_message"] = f"Memory/process error: {e}"
                logger.error(f"Likely memory error validating {file_path}. Stopping - larger files will also fail.")
                # Write final state before stopping
                with open(csv_path, "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
                    writer.writeheader()
                    writer.writerows(rows)
                return  # Stop processing
            row["schema_valid"] = False
            row["error_message"] = f"Schema validation error: {e}"

        # Write updated rows back after each file (incremental progress)
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
            writer.writeheader()
            writer.writerows(rows)

    logger.info(f"Schema validation complete for {csv_path}")


def validate_crosslinking_schemas():
    """Validate schemas for crosslinking files."""
    validate_schemas(CROSSLINKING_PATH, "crosslinking")


def validate_report_schemas():
    """Validate schemas for non-crosslinking files in the main report."""
    def is_non_crosslinking(row):
        return row["contains_MS1002511"] != "True"

    validate_schemas(REPORT_PATH, "non-crosslinking", filter_func=is_non_crosslinking)


if __name__ == "__main__":
    validate_crosslinking_schemas()
    validate_report_schemas()
