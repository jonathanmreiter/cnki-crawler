# Query day by day, storing raw page results as
# raw/YYYY-MM-DD-{page_number}.html
import multiprocessing as mp
import pandas as pd
import random
import requests

from datetime import date, timedelta

import query_json as qj
from utils import download_with_retry

INDEX_DESTINATION = "index-raw"


def check_if_page_exists(keyword, date, page_number):
    """Check if a specific page of search results exists locally."""
    destination = f"{INDEX_DESTINATION}/{keyword}-{date}-{page_number}.html"
    try:
        with open(destination, "r", encoding="utf-8") as f:
            return True
    except FileNotFoundError:
        return False


def check_if_page_is_empty(keyword, date, page_number):
    """Check if a specific page of search results is empty."""
    file_path = f"{INDEX_DESTINATION}/{keyword}-{date}-{page_number}.html"
    try:
        ret = pd.concat(pd.read_html(file_path, encoding="utf-8", extract_links="body"))
        return ret.empty
    except FileNotFoundError:
        return False


def get_date_range_list(start_date_str, end_date_str):
    """
    Generates a list of dates in 'YYYY-MM-DD' format within a given range.

    Args:
        start_date_str (str): The start date in 'YYYY-MM-DD' format.
        end_date_str (str): The end date in 'YYYY-MM-DD' format.

    Returns:
        list: A list of date strings in 'YYYY-MM-DD' format.
    """
    start_date = date.fromisoformat(start_date_str)
    end_date = date.fromisoformat(end_date_str)

    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.isoformat())
        current_date += timedelta(days=1)
    return date_list


def download_page(keyword, date, page_number):
    """Fetch a single page of search results from CNKI for a given keyword and date."""
    destination = f"{INDEX_DESTINATION}/{keyword}-{date}-{page_number}.html"
    url = "https://www.cnki.net/kns/Brief/GetGridTableHtml"
    headers = {
        "accept": "text/html, */*; q=0.01",
        "accept-language": "en-US,en;q=0.9,zh-TW;q=0.8,zh;q=0.7",
        "cache-control": "no-cache",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "pragma": "no-cache",
        "sec-ch-ua": '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "x-requested-with": "XMLHttpRequest",
        "Referer": "https://www.cnki.net/kns/AdvSearch?dbcode=CFLS&crossDbcodes=CJFQ,CDMD,CIPD,CCND,CYFD,CCJD,BDZK,SCOD,CISD,SNAD,CJFN",
    }

    # TODO: Figure out search sql parameter - it may be the plan cache or something
    payload = (
        "IsSearch=false&"
        "QueryJson=" + qj.create_query_json(keyword, date) + "&"
        "SearchSql=0645419CC2F0B23B004752D6FB74E5A8374A4C224EC0AD738D9AB73D028D38776D98A52F1CBB2A105331F81EA7F520CC21058EFA150CD069AC186C473C292F22B3EE5579AB61AA6452DACD2340DD05CF33F0786A5F7A7392CF26D95048D63689C964F9EEA2226FAB0D6CBBC1EA81724AC411A2F8E7D2857F80FCE36279E37C74520C01908734781B67E2802E6507BE21377A9D01891CB605E600CA6213A4065B7ECC3A29178520C2F3BECA9D31CEBD4F3E32C1121AA8ECCE4F56D6093798E46C6285BBE8AB4F4278BFE9C41DC58B61E3CE9A0C55A6C5A0C153E341CC825559A0820FB9A69F0C5B2FED06A282172FC3E0B7E39A7FDE17546BE2D3A778E87806311641A63978E71ECA83E1B8607A8CA1A4D8D48282391472F95FB98F3622864A3C4862BAE6035DCEBD08C2A861F8CD5817B7494243A3BC83C289BD4DA3A88EDA3C02A0367E9BFDFCC32AEE147289DD78538769688B7F353BC1D76039F2568158590FB1EEB5903A06497ADD15E71FD667F96CD76A25EC859AC714A3E2F9799BE8EFA0ED562817C382B4E00015D029C53A72958243F718F632C62B79283F51A34FDBA5352ECF225060FE518A09F29B20D35FAB1A3B15CA026025CE4EEC7C0937EEFC81382D277C650E07A84F554E8D4A6542315D3C2437735180090910B1CBE8B30FF083CE05EE8E8C025C368579A8E2708EFE830F498363803739E1A80623828BB30DFF6F5648F6C82628143274C08B52DF4D32F8DA352EF36F5F149B47A5957C133B9975D2CE91C71A1A58E062EE57C15CEA33E3CB26A04369B849F2F9255A14D68C02635A11B3779DD0EA755B2241E0B8D8F87CB7E5E4AD80F7546830F95823E8A31BC5E3F6292B5B1647F8E102B96C06164CCECA75EBCB43AE17BB1529CEB22244335D4C93E0ED7208C5DE1F76E2B41046462F995AAA1D2DD048C6DBC2B45F0B299CE7E89BDD0A5C45377B27F2102816648F3824105A1E5EFB8DFE3ED3FB3A3ECFDBACAFFDE37B9D3D5A650948014DFDE77558EF84E6F84B89C4203BB642F3E704FF735D8F38331C6762D71BFBD5C89605DE62F13B2718BE2B474FF76C331803473206CBE4E840E855741199259210DFB2CA937BA2EA0F5A53D26B15CBAFC3DAC39EE6A801A89AA9790AB03CEFECE52D204D5708E8806D0C4B2D7B65CBC78E0288D40A87969ACF39236C075DA56D2A5784604D691A9AD381F794026DA59BCA6CB3F5026223193FC6B7D16D4908C304571B235F3FF75F211957E15C5DF127919C4C9285BA433BB58472F10968EC7D22780F0A68BD6F9A787375B12E68A6A90F59761AB8B94FCB250F1D19C108998543D2A4DB905E03D61AA83B781A09EA21CC3514DAC8767BEDF65F840C7406BACF4E64D2AB328E66778F66718A1D7D9DE9E5E59B453C2640DE6EF6E4FA9FB18557D4235CB08F0AA9ED23A5E43A3C69A562CB8EBD2A5E127B8AF1952C45BA64946D99932E036ABC63E3E6FB948BD0E3C357885AC6229C9438F66809BAA8D1A53B8F3E665D3DE96DF2C1A1AC1ED8A43AA1ACC9D0B82ACBCABA04AFB6C6EDE12888704F274A03748FAEF8F8E7BD9BBEA595573F2271F8A76FBED940F8E7BFD6423CC3B69661197B088A3C9000C41234B332501A465BA879C8220CBFF112D952D371A5526176C4DDB0663B8C4525514FFA873D92E5164528227984668FB26823BCB87F70880B39F28C3D9AFF8A4520A2643A19F9687FB550184914DBF3B80037BFE277BF3E738A9C32CE62031447A7CD14350F8FFEC96099199F4EF8FBF0FECA38819CFB3F80F70868FAAAC5ED5F59F8710391069A9C8E0EED180DB582D0D51C374EB011E9AF45010C91DB53C77BD80A790A885293DFE95FAA0BF24A7D11CB27E91A40ED9B7C2F097FD67AFFF870F5EC89AF0EA5883DEFE2E015B1A668E7C837825438FA8BC2D67B4C99E1C52CF9D53E4F82C69C3E071A68A0B944ECE526FC33A66FC20B86111AE714B05CD2AF36469200BF1E17C90E265A6A267C7817A4D93956CEC8CEA89C148348CDECA7BB8E79AE1E773D5934F2E79311C512AA8FEBC9F40A24245F73D4D7E0EC0C3E71FA7C91714FC08E73BBEDAA1679C4AB8BE79B9C9AB6FA2017ED6DEF725C789648D31AAB38523E5C46E85471DB45490B5EA92B6C1CA68025CD81BBE01B01E5CE27271D487BEF2EC878F95685A228FFB4964BA40DD8BD38302FF77552376032FA5A865B64A245BE53A0535E094321FEE06DBFACE6CC9F0845D144DFE9F0D5CCA9B3F4AE8D831147084B31A2F8F5C84AD53F490518BBB7EF2FFBA5D86D805F92136DF1DE81530DB2812C7CAAFF6F7DA44A64535447ECC4CFF3E1D1A1324813E1D52A83CDD33C8C5C77A0553D2D9EEE9FBF4DF4774C263400993AE791769C0DB0E07202CFEC2CB3CD302690DA9BD085980E95D28C07C8F183D47659CA48CF55C4CE7737A9E9D26C3B23F5839426216AD6C296D686A7CE95CFB5066197F4B243BD1E5862DF143BB3F50FD928CB8F894ACA36520B08D27336ABF18F5BC738AA54B23349CF1B742D33813B756D9A68608E1A03F4DC5B4E3C466374A59D3CA2A83D30EC98FE7AFA2109C9E3ED5F9E0D681BF85226017B9485663597F04DFEF823ED10A34112E3073260060052AD471CA9D55A54D0107AE65AAF2E4F790C8BBC38B309736093FA46D6DEDFD4F2DD8FA74F963F7B04BC233768FA0E11D2AE44BF0579D52F1F60DF59BEC99421353DC0956A1884A509AEBDCE295438387608659F9B471B3E97B6466D1E8AA7F0FDC34764A21CA46226DF35C05BBB6011F8513CD65891C120DA9D33605031162C643E27190BB1C93557E50109FADC171767947FF205FCBC00E92A48DD919D31C5DF5DAFB754B984C2F861EA4AE7BB54139F567B6484A6BE8CA397109EF7FFD69F97465B9D51562E03BA92B4FD3DC77B9991B3EFF2895176530224&"
        "PageName=AdvSearch&"
        "HandlerId=" + str(random.randint(1, 100)) + "&"
        "DBCode=CFLS&"
        "KuaKuCodes=CJFQ%2CCDMD%2CCIPD%2CCCND%2CBDZK%2CCISD%2CSNAD%2CCCJD%2CGXDB_SECTION%2CCJFN%2CCCVD&"
        "CurPage=" + str(page_number) + "&"
        "RecordsCntPerPage=50&"
        "CurDisplayMode=listmode&"
        "CurrSortField=RELEVANT&"
        "CurrSortFieldType=desc&"
        "IsSentenceSearch=false&"
        "Subject="
    )
    response = download_with_retry(
        lambda: requests.post(url, headers=headers, data=payload, timeout=8)
    )

    # TODO: if more descriptive bad status, bail out with exception
    if response.status_code != 200:
        raise ValueError(
            f"Failed to retrieve search results: {response.status_code} {response.text}"
        )
    with open(destination, "w", encoding="utf-8") as f:
        f.write(response.text)


def download_all_pages(keyword, date, total_pages):
    """Download all pages of search results for a given keyword and date."""
    for page_number in range(1, total_pages + 1):
        if not check_if_page_exists(keyword, date, page_number):
            print(
                f"Downloading page {page_number} for keyword '{keyword}' on date '{date}'"
            )
            download_page(keyword, date, page_number)
            if check_if_page_is_empty(keyword, date, page_number):
                print(f"Page {page_number} is empty. Stopping further downloads.")
                break


def download_date_range(keyword, start_date_str, end_date_str, pages_per_day):
    """Download search results for a keyword over a range of dates."""
    date_list = get_date_range_list(start_date_str, end_date_str)
    for current_date in date_list:
        print(f"Processing date: {current_date}")
        download_all_pages(keyword, current_date, pages_per_day)


class Downloader(object):
    def __init__(self, start_date_str, end_date_str, pages_per_day):
        self.start_date_str = start_date_str
        self.end_date_str = end_date_str
        self.pages_per_day = pages_per_day

    def __call__(self, keyword):
        download_date_range(
            keyword, self.start_date_str, self.end_date_str, self.pages_per_day
        )


def iterate_through_corpus_and_download(
    corpus_file, start_date_str, end_date_str, pages_per_day
):
    with open(corpus_file, "r", encoding="utf-8") as f:
        keywords = []
        for line in f:
            for keyword in line.strip().split(","):
                keywords.append(keyword)
        processes = len(keywords)
        p = mp.Pool(processes)
        p.map(Downloader(start_date_str, end_date_str, pages_per_day), keywords)
        p.close()
        p.join()


if __name__ == "__main__":
    iterate_through_corpus_and_download(
        "corpus.txt", "2024mech-01-01", "2025-09-19", 24
    )
