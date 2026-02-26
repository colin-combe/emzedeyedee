import ftplib
import logging
import os
import time
from urllib.parse import urlparse
import requests


class SlowDownloadError(Exception):
    """Raised when download speed stays below threshold for too long."""
    pass


class SpeedMonitor:
    """Callback wrapper for retrbinary that aborts slow downloads.

    Wraps a file write callable. Every ``check_interval`` seconds it measures
    the average speed since the last check.  If the speed stays below
    ``min_speed`` bytes/sec for ``slow_timeout`` seconds continuously, raises
    ``SlowDownloadError``.
    """

    def __init__(self, write_func, *, min_speed: int = 10 * 1024,
                 check_interval: float = 30.0, slow_timeout: float = 1200.0):
        self._write = write_func
        self._min_speed = min_speed
        self._check_interval = check_interval
        self._slow_timeout = slow_timeout

        self._bytes_since_check = 0
        self._last_check = time.monotonic()
        self._slow_since: float | None = None  # None = not currently slow

    def __call__(self, data: bytes) -> None:
        self._write(data)
        self._bytes_since_check += len(data)

        now = time.monotonic()
        elapsed = now - self._last_check
        if elapsed < self._check_interval:
            return

        speed = self._bytes_since_check / elapsed
        self._bytes_since_check = 0
        self._last_check = now

        if speed < self._min_speed:
            if self._slow_since is None:
                self._slow_since = now
            elif now - self._slow_since >= self._slow_timeout:
                raise SlowDownloadError(
                    f"Download speed {speed:.0f} B/s below threshold "
                    f"{self._min_speed} B/s for over {self._slow_timeout}s"
                )
        else:
            self._slow_since = None

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
    for f in reversed(files):
        fetch_year(f)

def fetch_year(year):
    print (year)
    target_dir = base + '/' + year
    files = get_ftp_file_list(ip, target_dir)
    for f in reversed(files):
        fetch_month(year + '/' + f)

def fetch_month(year_month):
    target_dir = base + '/' + year_month
    files = get_ftp_file_list(ip, target_dir)
    for f in reversed(files):
        ymp = year_month + '/' + f
        fetch_project(ymp)

def fetch_project(year_month_project):
    target_dir = base + '/' + year_month_project
    full_path = '/' + target_dir
    print(f"Attempting to access FTP directory: {full_path}")
    ftp = get_ftp_login(ip)
    ftp.cwd(full_path)
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
    partial_path = path + '.partial'
    if os.path.exists(path):
        print(f"Skipping {file_name} (already exists)")
        return

    ftp_dir = '/' + base + '/' + ymp
    attempt = 0
    delay = base_delay

    while max_retries == 0 or attempt < max_retries:
        attempt += 1
        ftp = get_ftp_login(ip)

        # Check for existing partial download to resume
        existing_size = 0
        if os.path.exists(partial_path):
            existing_size = os.path.getsize(partial_path)

        try:
            ftp.cwd(ftp_dir)

            if existing_size > 0:
                f = open(partial_path, 'ab')
                try:
                    ftp.retrbinary("RETR " + file_name,
                                   SpeedMonitor(f.write),
                                   rest=existing_size)
                except ftplib.error_perm as e:
                    if _is_rest_error(e):
                        # Server rejected REST - start over
                        logger.warning(f"REST not supported, restarting download for {file_name}")
                        f.close()
                        f = open(partial_path, 'wb')
                        ftp.retrbinary("RETR " + file_name,
                                       SpeedMonitor(f.write))
                    else:
                        f.close()
                        raise
                f.close()
            else:
                with open(partial_path, 'wb') as f:
                    ftp.retrbinary("RETR " + file_name,
                                   SpeedMonitor(f.write))

            ftp.quit()
            os.rename(partial_path, path)
            return  # Success
        except ftplib.error_perm as e:
            # Permanent error (e.g., file not found) - don't retry
            _cleanup_partial_file(partial_path)
            try:
                ftp.quit()
            except Exception:
                pass
            error_msg = "%s: %s" % (file_name, e.args[0])
            logger.error(error_msg)
            raise e
        except (ConnectionResetError, OSError, EOFError, ftplib.error_temp,
                SlowDownloadError) as e:
            # Transient error - keep partial file for resume
            try:
                ftp.quit()
            except Exception:
                pass
            logger.error(f"Download failed for {file_name} on attempt {attempt}: {type(e).__name__}: {e}")

            if max_retries != 0 and attempt >= max_retries:
                logger.error(f"Max retries ({max_retries}) exceeded for {file_name}")
                _cleanup_partial_file(partial_path)
                raise

            # Exponential backoff with jitter
            jitter = delay * 0.1 * (2 * (time.time() % 1) - 1)
            current_delay = min(delay + jitter, max_delay)
            logger.info(f"Retrying {file_name} in {current_delay:.2f}s (attempt {attempt + 1})")
            print(f"  Retrying in {current_delay:.1f}s...")
            time.sleep(current_delay)
            delay = min(delay * 2, max_delay)

    _cleanup_partial_file(partial_path)
    raise ftplib.error_temp(f"Download failed for {file_name} after all retries")


def _is_rest_error(exc: ftplib.error_perm) -> bool:
    """Check if an error_perm was caused by a rejected REST command."""
    msg = str(exc).lower()
    return 'rest' in msg or msg.startswith('501') or msg.startswith('502')


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


def pxd_accession_to_ftp_dir(px_accession: str):
    """Get FTP location from PRIDE API and process dataset."""
    px_url = f"https://www.ebi.ac.uk/pride/ws/archive/v3/projects/{px_accession}/files"
    logger.info(f"GET request to PRIDE API: {px_url}")
    print(f"Querying PRIDE API for {px_accession}")
    pride_response = requests.get(px_url, timeout=30)

    if pride_response.status_code == 200:
        logger.info("PRIDE API returned status code 200")
        pride_json = pride_response.json()

        if pride_json:
            for protocol in pride_json[0].get("publicFileLocations", []):
                if protocol["name"] == "FTP Protocol":
                    ftp_full_url = protocol["value"]
                    # print(f"  FTP URL from API: {ftp_full_url}")
                    parsed_url = urlparse(ftp_full_url)
                    # print(f"  Parsed path: {parsed_url.path}")
                    path_parts = parsed_url.path.split("/")
                    # print(f"  Path parts: {path_parts}")

                    # Find the PXD accession in the path and extract YEAR/MONTH/PXD
                    try:
                        pxd_index = path_parts.index(px_accession)
                        # Get year (2 positions before PXD) and month (1 position before PXD)
                        if pxd_index >= 2:
                            year = path_parts[pxd_index - 2]
                            month = path_parts[pxd_index - 1]
                            constructed_path = f"{year}/{month}/{px_accession}"
                            # print(f"  Constructed path: {constructed_path}")
                            return constructed_path
                        else:
                            raise ValueError(f"Not enough path components before PXD at index {pxd_index}")
                    except ValueError as e:
                        raise ValueError(f"Could not find {px_accession} in FTP path: {parsed_url.path}") from e

        raise ValueError(
            f"No FTP location found in PRIDE API response for {px_accession}"
        )
    else:
        raise ValueError(
            f"PRIDE API returned status code {pride_response.status_code}"
        )

# all_years()
# fetch_project('2012/12/PXD000039')

def project(projAcc):
    fetch_project(pxd_accession_to_ftp_dir(projAcc))


# project('PXD042282')
# project('PXD043595')
# project('PXD051742')
# project('PXD061667')
# project('PXD063329')
# project('PXD064792')
# project('PXD065365')
# project('PXD065516')
# project('PXD065858')
# project('PXD065859')
# project('PXD065869')
# project('PXD065870')
# project('PXD065871')
# project('PXD065912')
# project('PXD065946')
# project('PXD065949')
# project('PXD065956')
# project('PXD065958')
# project('PXD065961')
# project('PXD001677')
# project('PXD003718')
project('PXD004154')
project('PXD004583')
project('PXD004722')
project('PXD006079')
project('PXD006574')
project('PXD006928')
project('PXD006938')
project('PXD007716')
project('PXD007836')
project('PXD009079')
project('PXD009128')
project('PXD009641')
project('PXD012225')
project('PXD012466')
project('PXD013890')
project('PXD013896')
project('PXD013897')
project('PXD013899')
project('PXD014142')
project('PXD014821')
project('PXD016224')
project('PXD016256')
project('PXD016442')
project('PXD016446')
project('PXD016448')
project('PXD016487')
project('PXD017290')
project('PXD017792')
project('PXD017873')
project('PXD018291')
project('PXD018600')
project('PXD018687')
project('PXD018701')
project('PXD019017')
project('PXD019713')
project('PXD019771')
project('PXD019868')
project('PXD019944')
project('PXD020418')
project('PXD020666')
project('PXD020958')
project('PXD022163')
project('PXD023525')
project('PXD024065')
project('PXD024253')
project('PXD024373')
project('PXD025066')
project('PXD025728')
project('PXD026101')
project('PXD026674')
project('PXD026829')
project('PXD027149')
project('PXD028685')
project('PXD028919')
project('PXD031159')
project('PXD031385')
project('PXD033004')
project('PXD033055')
project('PXD033175')
project('PXD033615')
project('PXD034393')
project('PXD035655')
project('PXD038128')
project('PXD040267')
project('PXD041334')
project('PXD041955')
project('PXD042549')
project('PXD044574')
project('PXD046392')
project('PXD046634')
project('PXD046895')
project('PXD047767')
project('PXD051423')
project('PXD051588')
project('PXD051661')
project('PXD051886')
project('PXD052584')
project('PXD053253')
project('PXD053415')
project('PXD053636')
project('PXD055077')
project('PXD055147')
project('PXD055405')
project('PXD055411')
project('PXD055603')
project('PXD056510')
project('PXD059974')
project('PXD060469')
project('PXD062002')
project('PXD065573')
project('PXD068080')
project('PXD004473')
project('PXD005786')
project('PXD008680')
project('PXD012759')
project('PXD014359')
project('PXD014520')
project('PXD014523')
project('PXD015037')
project('PXD018935')
project('PXD026603')
project('PXD027655')
project('PXD028039')
project('PXD029252')
project('PXD055169')
project('PXD059096')
project('PXD065782')
project('PXD019437')
project('PXD022936')
project('PXD031632')
project('PXD050457')
project('PXD039609')
project('PXD031755')
project('PXD024148')
project('PXD023533')
project('PXD038060')
project('PXD042173')
project('PXD036833')
project('PXD053341')
project('PXD059766')
project('PXD054720')
project('PXD020407')
project('PXD056910')
project('PXD021417')
project('PXD049195')
project('PXD063839')
project('PXD033366')
project('PXD035522')
project('PXD019120')
project('PXD035519')
project('PXD035362')
project('PXD022360')
project('PXD035508')
project('PXD062462')
project('PXD066083')
project('PXD066251')

#warnign 2013/10/PXD000323

# fetch_month('2013/11')
# fetch_month('2013/12')
# fetch_year('2014')
# fetch_year('2015')
# fetch_year('2016')
# fetch_year('2017')
# fetch_year('2018')
# fetch_year('2019')
# fetch_year('2020')
# fetch_year('2021')
# fetch_year('2022')
# fetch_year('2023')
# fetch_year('2024')
# fetch_year('2025')
# fetch_year('2026')

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
