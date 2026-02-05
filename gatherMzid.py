import ftplib
import logging
import os
import time

# count id files
mzId_count = 0
# logging
logging.basicConfig(level=logging.ERROR, format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)
# config
ip = "ftp.pride.ebi.ac.uk"
base = "pride/data/archive"
temp_dir = os.path.expanduser('~') + "/mzid_store/"
os.makedirs(temp_dir, exist_ok=True)


def all_years():
    files = get_ftp_file_list(ip, base)
    for f in files:
        fetch_year(f)

def fetch_year(year):
    print (year)
    target_dir = base + '/' + year
    files = get_ftp_file_list(ip, target_dir)
    for f in files:
        fetch_month(year + '/' + f)

def fetch_month(year_month):
    target_dir = base + '/' + year_month
    files = get_ftp_file_list(ip, target_dir)
    for f in files:
        ymp = year_month + '/' + f
        fetch_project(ymp)

def fetch_project(year_month_project):
    target_dir = base + '/' + year_month_project
    ftp = get_ftp_login(ip)
    ftp.cwd('/' + target_dir)
    print ('>> ' + year_month_project)

    # Get detailed listing to distinguish files from directories
    listing = []
    ftp.retrlines('LIST', listing.append)
    ftp.quit()

    for line in listing:
        # Parse LIST output: first char is 'd' for directory, '-' for file
        if line.startswith('-') and 'mzid' in line.lower():
            # Extract filename: skip first 8 fields (permissions, links, owner, group, size, month, day, year/time)
            # Then everything after is the filename (which may contain spaces)
            parts = line.split(None, 8)  # Split on whitespace, max 9 parts
            if len(parts) >= 9:
                filename = parts[8]
                # Skip .mgf files
                if filename.lower().endswith('.mgf'):
                    continue
                print(filename)
                fetch_file(year_month_project, filename)

def fetch_file(ymp, file_name, max_retries: int = 0, base_delay: float = 1.0, max_delay: float = 300.0):
    os.makedirs(temp_dir + ymp, exist_ok=True)
    path = temp_dir + ymp + '/' + file_name
    if os.path.exists(path):
        print(f"Skipping {file_name} (already exists)")
        return

    ftp_dir = '/' + base + '/' + ymp
    attempt = 0
    delay = base_delay

    while max_retries == 0 or attempt < max_retries:
        attempt += 1
        ftp = get_ftp_login(ip)

        # fetch mzId file from pride
        try:
            ftp.cwd(ftp_dir)
            with open(path, 'wb') as f:
                ftp.retrbinary("RETR " + file_name, f.write)
            ftp.quit()
            return  # Success
        except ftplib.error_perm as e:
            # Permanent error (e.g., file not found) - don't retry
            _cleanup_partial_file(path)
            try:
                ftp.quit()
            except Exception:
                pass
            error_msg = "%s: %s" % (file_name, e.args[0])
            logger.error(error_msg)
            raise e
        except (ConnectionResetError, OSError, EOFError, ftplib.error_temp) as e:
            # Transient error - clean up and retry
            _cleanup_partial_file(path)
            try:
                ftp.quit()
            except Exception:
                pass
            logger.error(f"Download failed for {file_name} on attempt {attempt}: {type(e).__name__}: {e}")

            if max_retries != 0 and attempt >= max_retries:
                logger.error(f"Max retries ({max_retries}) exceeded for {file_name}")
                raise

            # Exponential backoff with jitter
            jitter = delay * 0.1 * (2 * (time.time() % 1) - 1)
            current_delay = min(delay + jitter, max_delay)
            logger.info(f"Retrying {file_name} in {current_delay:.2f}s (attempt {attempt + 1})")
            print(f"  Retrying in {current_delay:.1f}s...")
            time.sleep(current_delay)
            delay = min(delay * 2, max_delay)

    raise ftplib.error_temp(f"Download failed for {file_name} after all retries")


def _cleanup_partial_file(path: str):
    """Remove a partially downloaded file if it exists."""
    if os.path.exists(path):
        try:
            os.remove(path)
            logger.debug(f"Cleaned up partial file: {path}")
        except OSError as e:
            logger.warning(f"Failed to clean up partial file {path}: {e}")

def get_ftp_login(ftp_ip: str, max_retries: int = 10, base_delay: float = 1.0, max_delay: float = 300.0) -> ftplib.FTP:
    """Log in to an FTP server with exponential backoff.

    Args:
        ftp_ip: The FTP server IP address.
        max_retries: Maximum number of retry attempts (0 for infinite).
        base_delay: Initial delay in seconds before first retry.
        max_delay: Maximum delay in seconds between retries.

    Returns:
        An authenticated FTP connection.

    Raises:
        ftplib.all_errors: If max_retries is exceeded.
    """
    attempt = 0
    delay = base_delay

    while max_retries == 0 or attempt < max_retries:
        attempt += 1
        logger.debug(f"FTP login attempt {attempt} to {ftp_ip}")

        try:
            logger.debug(f"Creating FTP connection to {ftp_ip}")
            ftp = ftplib.FTP(ftp_ip)
            logger.debug(f"FTP connection established, attempting anonymous login")
            ftp.login()  # Uses password: anonymous@
            logger.debug(f"FTP login successful to {ftp_ip} on attempt {attempt}")
            return ftp
        except ftplib.all_errors as e:
            logger.error(f"FTP login failed to {ftp_ip} on attempt {attempt}: {type(e).__name__}: {e}")

            if max_retries != 0 and attempt >= max_retries:
                logger.error(f"Max retries ({max_retries}) exceeded for FTP login to {ftp_ip}")
                raise

            # Calculate delay with exponential backoff and jitter
            jitter = delay * 0.1 * (2 * (time.time() % 1) - 1)  # +/- 10% jitter
            current_delay = min(delay + jitter, max_delay)
            logger.debug(f"Waiting {current_delay:.2f}s before retry (base delay: {delay:.2f}s, max: {max_delay}s)")
            time.sleep(current_delay)

            # Exponential backoff: double the delay for next attempt
            delay = min(delay * 2, max_delay)
            logger.debug(f"Next retry delay set to {delay:.2f}s")

    # This should be unreachable when max_retries=0, but satisfies type checker
    raise ftplib.error_temp("FTP login failed after all retries")

def get_ftp_file_list(ftp_ip: str, ftp_dir: str) -> list[str]:
    """Get a list of files from an FTP directory."""
    ftp = get_ftp_login(ftp_ip)
    try:
        ftp.cwd(ftp_dir)
    except ftplib.error_perm as e:
        logger.error(f"{ftp_dir}: {e}")
        ftp.quit()
        raise e
    try:
        return ftp.nlst()
    except ftplib.error_perm as e:
        if str(e) == "550 No files found":
            logger.info(f"FTP: No files in {ftp_dir}")
        else:
            logger.error(f"{ftp_dir}: {e}")
        raise e
    finally:
        ftp.close()


# all_years()
# fetch_project('2012/12/PXD000039')


#warnign 2013/10/PXD000323

# fetch_month('2013/11')
# fetch_month('2013/12')
# fetch_year('2014')
# fetch_year('2015')
# fetch_year('2016')
fetch_year('2017')
fetch_year('2018')
fetch_year('2019')
fetch_year('2020')
fetch_year('2021')
fetch_year('2022')
fetch_year('2023')
fetch_year('2024')
fetch_year('2025')
fetch_year('2026')

# # test_loop.year('2018')
# # test_loop.year('2017')
# # test_loop.year('2016')
#
# test_loop.month('2016/08')
# test_loop.month('2016/07')
# test_loop.month('2016/06')
# test_loop.month('2016/05')
# test_loop.month('2016/04')
# test_loop.month('2016/03')
# test_loop.month('2016/02')
# test_loop.month('2016/01')
#
# test_loop.year('2015')
# test_loop.year('2014')
# test_loop.year('2013')
# test_loop.month('2012/12')
#
# # test_loop.project("2018/05/PXD005015") # no attribute 'tag', problems is with attributes containing single quote mark
# # test_loop.project("2018/07/PXD007714") # no attribute 'tag', also 2018/09/PXD009640
# # test_loop.project("2018/06/PXD009747") # odd missing file # compare 2018/07/PXD009603
#
# # test_loop.project("2016/08/PXD004741") # zip archive error
#
# # test_loop.project("2018/04/PXD008493") # massive 2.9Gb mzML, very slow, takes days
#
#
# # test_loop.project("2018/06/PXD010000")
# # test_loop.project("2018/11/PXD009966")
# # test_loop.project("2018/10/PXD010121") # good one, raw file with MGF accession number
#
#
# # mzML
# # test_loop.project("2017/11/PXD007748")
# # test_loop.project("2016/11/PXD004785")
# # test_loop.project("2016/05/PXD002967")
# # test_loop.project("2016/09/PXD004499")
# # test_loop.project("2015/06/PXD002045")
# # test_loop.project("2017/08/PXD007149")
# # test_loop.project("2015/06/PXD002048")
# # test_loop.project("2015/06/PXD002047")
# # test_loop.project("2014/11/PXD001267")
#
# # 2015/06/PXD002046
# # 2014/09/PXD001006
# # 2014/09/PXD001000
# # 2016/09/PXD002317
# # 2014/09/PXD000966
# # 2015/06/PXD002044
# # 2015/06/PXD002043
# # 2015/06/PXD002042
# # 2015/06/PXD002041
# # 2016/06/PXD004163
# # 2015/05/PXD002161
# # 2018/01/PXD007913
# # 2017/11/PXD006204
# # 2015/07/PXD002089
# # 2015/07/PXD002088
# # 2015/07/PXD002087
# # 2015/07/PXD002086
# # 2017/07/PXD002901
# # 2015/07/PXD002085
# # 2017/11/PXD007689
# # 2015/07/PXD002084
# # 2015/05/PXD002161
# # 2015/05/PXD002161
# # 2015/07/PXD002083
# # 2015/07/PXD002082
# # 2015/07/PXD002081
# # 2015/07/PXD002080
# # 2015/06/PXD002050
# # 2015/06/PXD002049
#
# # sim-xl
# # test_loop.project("2017/05/PXD006574")
# # test_loop.project("2015/02/PXD001677")
#
# # missing file
# # test_loop.project("2013/09/PXD000443")
#
# # prob
# # test_loop.project("2014/04/PXD000579") # missing file name

print("mzId count:" + str(mzId_count))

# @staticmethod
# def get_pride_info (pxd):
#     time.sleep(1)
#     try:
#         prideAPI = urllib.urlopen('https://www.ebi.ac.uk:443/pride/ws/archive/project/' + pxd).read()
#         pride = json.loads(prideAPI)
#         return pride
#     except Exception:
#         print ("failed to get " + pxd + "from pride api. Will try again in 5 secs.")
#         time.sleep(5)
#         return TestLoop.get_pride_info(pxd)
