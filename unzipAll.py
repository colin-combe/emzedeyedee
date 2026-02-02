import gzip
import logging
import os
import shutil
import zipfile

# logging - same style as gatherMzid.py
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)

base_dir = "/home/cc/mzid_store"


def unzip_all():
    """Walk through all subdirectories and unzip any zip or gzip archives."""
    extracted_files = []
    failed_extractions = []

    logger.debug(f"Starting extraction walk in {base_dir}")

    for root, dirs, files in os.walk(base_dir):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            lower_name = file_name.lower()

            if lower_name.endswith('.zip'):
                logger.debug(f"Found zip archive: {file_path}")
                result = extract_zip(file_path)
                if result:
                    extracted_files.extend(result)
                else:
                    failed_extractions.append(file_path)

            elif lower_name.endswith('.gz') or lower_name.endswith('.gzip'):
                logger.debug(f"Found gzip archive: {file_path}")
                result = extract_gzip(file_path)
                if result:
                    extracted_files.append(result)
                else:
                    failed_extractions.append(file_path)

    logger.debug(f"Extraction complete. Extracted {len(extracted_files)} files, {len(failed_extractions)} failures")

    # Report failures
    if failed_extractions:
        logger.error(f"Failed to extract {len(failed_extractions)} archives:")
        for path in failed_extractions:
            logger.error(f"  - {path}")

    # Report non-.mzid files
    non_mzid_files = [f for f in extracted_files if not f.lower().endswith('.mzid')]
    if non_mzid_files:
        print(f"\nExtracted files that don't end with .mzid ({len(non_mzid_files)}):")
        for f in non_mzid_files:
            print(f"  - {f}")
    else:
        print("\nAll extracted files end with .mzid")

    return extracted_files, failed_extractions


def extract_zip(zip_path: str) -> list[str] | None:
    """Extract a zip archive to its containing directory.

    Args:
        zip_path: Path to the zip file.

    Returns:
        List of extracted file paths, or None on failure.
    """
    extract_dir = os.path.dirname(zip_path)
    logger.debug(f"Extracting zip {zip_path} to {extract_dir}")

    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            # Check for valid zip
            if zf.testzip() is not None:
                logger.error(f"Zip file is corrupted: {zip_path}")
                return None

            members = zf.namelist()
            logger.debug(f"Zip contains {len(members)} members")

            zf.extractall(extract_dir)
            extracted = [os.path.join(extract_dir, m) for m in members if not m.endswith('/')]
            logger.debug(f"Successfully extracted {len(extracted)} files from {zip_path}")
            return extracted

    except zipfile.BadZipFile as e:
        logger.error(f"Bad zip file {zip_path}: {type(e).__name__}: {e}")
        return None
    except PermissionError as e:
        logger.error(f"Permission denied extracting {zip_path}: {type(e).__name__}: {e}")
        return None
    except OSError as e:
        logger.error(f"OS error extracting {zip_path}: {type(e).__name__}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error extracting {zip_path}: {type(e).__name__}: {e}")
        return None


def extract_gzip(gzip_path: str) -> str | None:
    """Extract a gzip archive to its containing directory.

    Args:
        gzip_path: Path to the gzip file.

    Returns:
        Path to extracted file, or None on failure.
    """
    # Remove .gz or .gzip extension to get output filename
    if gzip_path.lower().endswith('.gzip'):
        output_path = gzip_path[:-5]
    else:
        output_path = gzip_path[:-3]

    logger.debug(f"Extracting gzip {gzip_path} to {output_path}")

    try:
        with gzip.open(gzip_path, 'rb') as f_in:
            with open(output_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        logger.debug(f"Successfully extracted {gzip_path} to {output_path}")
        return output_path

    except gzip.BadGzipFile as e:
        logger.error(f"Bad gzip file {gzip_path}: {type(e).__name__}: {e}")
        return None
    except PermissionError as e:
        logger.error(f"Permission denied extracting {gzip_path}: {type(e).__name__}: {e}")
        return None
    except OSError as e:
        logger.error(f"OS error extracting {gzip_path}: {type(e).__name__}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error extracting {gzip_path}: {type(e).__name__}: {e}")
        return None


if __name__ == "__main__":
    extracted, failed = unzip_all()
    print(f"\nSummary: {len(extracted)} files extracted, {len(failed)} failures")