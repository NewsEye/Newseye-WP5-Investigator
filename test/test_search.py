import unittest
import requests
from read_config import read_config
from test_utilities import TestUtility


class TestSearch(TestUtility):
    task_result = "search_task_result.json"
    utype = "search"
    payload = '{"q": "Republik AND Flüchtlinge AND Australien","fq": "member_of_collection_ids_ssim:arbeiter_zeitung","mm": 3, "force_refresh":"T"}'.encode(
        "utf-8"
    )
