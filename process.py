import pandas as pd
import os

RAW_DIRECTORY = "index-raw"
PROCESSED_DIRECTORY = "index-processed"


def check_if_page_exists(file_path):
    """Check if a specific page of search results exists locally."""
    destination = f"{PROCESSED_DIRECTORY}/refined_{os.path.basename(file_path).replace('.html', '.csv')}"
    try:
        with open(destination, "r", encoding="utf-8") as f:
            return True
    except FileNotFoundError:
        return False


def list_all_files(directory_path):
    """
    Lists all files in a specified directory.
    """
    files = []
    for entry in os.listdir(directory_path):
        full_path = os.path.join(directory_path, entry)
        if os.path.isfile(full_path):
            files.append(full_path)
    return files


def parse_cnki_index(file_path, processed_directory):
    try:
        if not check_if_page_exists(file_path):
            raw = pd.concat(
                pd.read_html(file_path, encoding="utf-8", extract_links="body")
            )
            ret = pd.DataFrame(
                {
                    "scrapeId": raw["Unnamed: 0"].str[0] + "_" + file_path,
                    "title": raw["Title"].str[0],
                    "titleLink": raw["Title"].str[1],
                    "author": raw["Author"].str[0],
                    "source": raw["Source"].str[0],
                    "sourceLink": raw["Source"].str[1],
                    "publicationDate": raw["Publication Date"].str[0],
                    "database": raw["Database"].str[0],
                    "cites": raw["Cites"].str[0],
                    "downloads": raw["Downloads"].str[0],
                    "options": raw["Options"].str[0],
                    "optionsLink": raw["Options"].str[1],
                }
            )
            ret.to_csv(
                f"{processed_directory}/refined_{os.path.basename(file_path).replace('.html', '.csv')}",
                sep="|",
                index=False,
            )
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return pd.DataFrame()


def parse_all_indices():
    indices = []
    for file_path in list_all_files(RAW_DIRECTORY):
        df_list = parse_cnki_index(file_path, PROCESSED_DIRECTORY)
        indices.append(df_list)
    return indices


def process():
    parse_all_indices()
    indices = []
    for file_path in list_all_files(PROCESSED_DIRECTORY):
        if file_path.endswith(".csv"):
            df = pd.read_csv(file_path, sep="|")
            if not df.empty:
                indices.append(df)
    combined_df = pd.concat(indices, ignore_index=True)
    combined_df.to_csv("enriched/001_combined.csv", sep="|", index=False)


process()
