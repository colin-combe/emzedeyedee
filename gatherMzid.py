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
    files = get_ftp_file_list(ip, target_dir)
    print ('>> ' + year_month_project)
    for f in files:
        if 'mzid' in f.lower():
            print(f)
            fetch_file(year_month_project, f)
            break

def fetch_file(ymp, file_name):
    os.makedirs(temp_dir + ymp, exist_ok=True)
    path = temp_dir + ymp + '/' + file_name
    if os.path.exists(path):
        print(f"Skipping {file_name} (already exists)")
        return

    ftp_dir = '/' + base + '/' + ymp
    ftp = get_ftp_login(ip)

    # fetch mzId file from pride
    try:
        ftp.cwd(ftp_dir)
        ftp.retrbinary("RETR " + file_name, open(path, 'wb').write)
    except ftplib.error_perm as e:
        ftp.quit()
        error_msg = "%s: %s" % (file_name, e.args[0])
        logger.error(error_msg)
        raise e
    ftp.quit()

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
fetch_year('2025')
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
