"""schema_validate.py - Validate an mzIdentML file against XSD schema."""

import os
from multiprocessing import Pool
from typing import List, Tuple

from lxml import etree

# Path to schema files relative to this script
SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "..", "mzIdentML", "schema")

# Supported schema files
SUPPORTED_SCHEMAS = [
    "mzIdentML1.0.0.xsd",
    "mzIdentML1.1.0.xsd",
    "mzIdentML1.1.1.xsd",
    "mzIdentML1.2.0.xsd",
    "mzIdentML1.3.0.xsd",
]


def schema_validate(xml_file: str) -> bool:
    """Validate an mzIdentML file against its declared schema.

    Runs validation in a subprocess to ensure memory is fully released
    after validation completes (lxml/libxml2 holds memory otherwise).

    Args:
        xml_file: Path to the mzIdentML file

    Returns:
        True if the XML is valid, False otherwise
    """
    with Pool(1) as pool:
        success, schema_version, messages = pool.apply(_schema_validate_impl, (xml_file,))

    for msg in messages:
        print(msg)

    return success


def _extract_schema_version(schema_fname: str) -> str | None:
    """Extract version string from schema filename (e.g., '1.2.0' from 'mzIdentML1.2.0.xsd')."""
    if schema_fname.startswith("mzIdentML") and schema_fname.endswith(".xsd"):
        return schema_fname[9:-4]  # Remove 'mzIdentML' prefix and '.xsd' suffix
    return None


def _schema_validate_impl(xml_file: str) -> Tuple[bool, str | None, List[str]]:
    """Internal implementation of schema validation (runs in subprocess).

    Returns:
        Tuple of (success, schema_version, list of messages)
    """
    messages = []
    schema_version = None

    # Parse the XML file
    with open(xml_file, "r") as xml:
        xml_doc = etree.parse(xml)

    # Extract schema location from the XML (xsi:schemaLocation or xsi:noNamespaceSchemaLocation)
    root = xml_doc.getroot()
    schema_location = root.attrib.get(
        "{http://www.w3.org/2001/XMLSchema-instance}schemaLocation"
    )

    if not schema_location:
        schema_location = root.attrib.get(
            "{http://www.w3.org/2001/XMLSchema-instance}noNamespaceSchemaLocation"
        )

    if not schema_location:
        messages.append("No schema location found in the XML document.")
        return False, None, messages

    # The schemaLocation attribute may contain multiple namespaces and schema locations.
    # Typically, it's formatted as "namespace schemaLocation" pairs.
    schema_parts = schema_location.split()
    if len(schema_parts) % 2 != 0:
        messages.append("Invalid schema location format.")
        return False, None, messages

    # Assuming a single namespace-schema pair for simplicity
    schema_url = (
        schema_parts[1] if len(schema_parts) == 2 else schema_parts[-1]
    )

    # just take the file name from the url
    schema_fname = schema_url.split("/")[-1]
    schema_version = _extract_schema_version(schema_fname)

    if schema_fname not in SUPPORTED_SCHEMAS:
        messages.append(f"Unsupported schema: {schema_fname}")
        return False, schema_version, messages

    try:
        schema_path = os.path.join(SCHEMA_DIR, schema_fname)
        with open(schema_path, "r") as schema_file:
            schema_root = etree.XML(schema_file.read())
        schema = etree.XMLSchema(schema_root)

        if schema.validate(xml_doc):
            return True, schema_version, messages
        else:
            messages.append("XML is invalid. First 20 errors:")
            for error in schema.error_log[:20]:
                messages.append(
                    f"Error: {error.message}, Line: {error.line}"
                )
            return False, schema_version, messages

    except FileNotFoundError:
        messages.append(f"Schema file not found: {schema_path}")
        return False, schema_version, messages


# Default timeout for schema validation (seconds)
VALIDATION_TIMEOUT = 600


def schema_validate_with_messages(xml_file: str, timeout: int = VALIDATION_TIMEOUT) -> Tuple[bool, str | None, List[str]]:
    """Validate an mzIdentML file and return validation messages.

    Like schema_validate(), but returns error messages instead of printing them.
    Runs validation in a subprocess to ensure memory is fully released.

    Args:
        xml_file: Path to the mzIdentML file
        timeout: Maximum seconds to wait for validation (default 60)

    Returns:
        Tuple of (success, schema_version, list of error/info messages)
    """
    with Pool(1) as pool:
        async_result = pool.apply_async(_schema_validate_impl, (xml_file,))
        try:
            success, schema_version, messages = async_result.get(timeout=timeout)
        except TimeoutError:
            pool.terminate()
            return False, None, [f"Validation timed out after {timeout}s"]
    return success, schema_version, messages
