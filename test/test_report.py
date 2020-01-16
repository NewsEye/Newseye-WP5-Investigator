from test_utilities import TestUtility
import requests
import os
from read_config import read_config


class TestReport(TestUtility):
    # TODO: replace target_search with search_query once new version is deployed

    utype = "report"
    task_result = "report_task_result.json"
    payload = '{"search_query": {"q": "Flüchtlinge"},"utility": "common_facet_values","utility_parameters": {"n": 5},"force_refresh":"T"}'.encode(
        "utf-8"
    )

    def setUp(self):
        self.headers, self.url = read_config()
        self.response = requests.request(
            "POST",
            self.url,
            data=self.payload,
            headers={"content-type": "application/json", **self.headers},
        ).json()
        self.headers, self.url = read_config("report")
