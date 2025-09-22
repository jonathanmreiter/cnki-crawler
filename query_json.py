import json
import urllib.parse


def create_query_json(keyword: str, date: str) -> dict:
    """Create the QueryJson dictionary for CNKI search."""
    ret = {
        "Platform": "",
        "DBCode": "CFLS",
        "KuaKuCode": "CJFQ,CDMD,CIPD,CCND,BDZK,CISD,SNAD,CCJD,GXDB_SECTION,CJFN,CCVD",
        "QNode": {
            "QGroup": [
                {
                    "Key": "Subject",
                    "Title": "",
                    "Logic": 4,
                    "Items": [],
                    "ChildItems": [
                        {
                            "Key": "input[data-tipid=gradetxt-1]",
                            "Title": "Title, Keyword and Abstract ",
                            "Logic": 0,
                            "Items": [
                                {
                                    "Key": "",
                                    "Title": keyword,
                                    "Logic": 1,
                                    "Name": "TKA",
                                    "Operate": "=",
                                    "Value": keyword,
                                    "ExtendType": 1,
                                    "ExtendValue": "中英文对照",
                                    "Value2": "",
                                }
                            ],
                            "ChildItems": [],
                        }
                    ],
                },
                {
                    "Key": "ControlGroup",
                    "Title": "",
                    "Logic": 1,
                    "Items": [],
                    "ChildItems": [
                        {
                            "Key": "span[value=PT]",
                            "Title": "",
                            "Logic": 1,
                            "Items": [
                                {
                                    "Key": "span[value=PT]",
                                    "Title": "Publication Date",
                                    "Logic": 1,
                                    "Name": "PT",
                                    "Operate": "",
                                    "Value": date,
                                    "ExtendType": 2,
                                    "ExtendValue": "",
                                    "Value2": date,
                                    "BlurType": "",
                                }
                            ],
                            "ChildItems": [],
                        }
                    ],
                },
            ]
        },
        "ExScope": 1,
        "CodeLang": "",
    }
    return urllib.parse.quote_plus(
        json.dumps(ret, ensure_ascii=False, separators=(",", ":"))
    )
