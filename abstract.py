import pandas as pd
import requests

from utils import download_with_retry, url_to_params

ABSTRACT_DESTINATION = "abstract-raw"
INDEX_PROCESSED = "index-processed"


def check_if_page_exists(destination):
    """Check if a specific page of search results exists locally."""
    try:
        with open(destination, "r", encoding="utf-8") as f:
            return True
    except FileNotFoundError:
        return False


def get_abstract(dbcode, dbname, filename):
    try:
        url = f"https://www.cnki.net/KCMS/detail/detail.aspx?dbcode={dbcode}&dbname={dbname}&filename={filename}"
        destination = f"{ABSTRACT_DESTINATION}/{dbcode}-{dbname}-{filename}.html"
        if not check_if_page_exists(destination):
            print(f"{url} -> {destination}")
            response = download_with_retry(lambda: requests.get(url))
            with open(destination, "w", encoding="utf-8") as f:
                f.write(response.text)
    except requests.RequestException as e:
        return f"Error fetching abstract: {e}"


def get_all_abstracts_from_processed_file(processed_file):
    with open(processed_file, "r", encoding="utf-8") as f:
        df = pd.read_csv(f, sep="|")
        for _, row in df.iterrows():
            title_link = row["titleLink"]
            if title_link:
                dbcode, dbname, filename = url_to_params(title_link)
                get_abstract(dbcode, dbname, filename)


get_all_abstracts_from_processed_file(f"{INDEX_PROCESSED}/001_combined.csv")
