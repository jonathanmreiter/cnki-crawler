import time


def download_with_retry(fn, retries=3, delay=1):
    """Download with retries and delay between attempts."""
    for attempt in range(retries):
        try:
            return fn()
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
    raise Exception("All download attempts failed.")


def url_to_params(url):
    """Extract dbcode, dbname, and filename from the given URL."""
    from urllib.parse import urlparse, parse_qs

    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)

    dbcode = query_params.get("DbCode", [""])[0]
    dbname = query_params.get("DbName", [""])[0]
    filename = query_params.get("FileName", [""])[0]

    return dbcode, dbname, filename
