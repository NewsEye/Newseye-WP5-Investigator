from test_utilities import TestUtility
import requests
import os
from read_config import read_config

class TestReport(TestUtility):
    # TODO: replace target_search with search_query once new version is deployed
    
    utype="report"
    task_result = "report_task_result.json"
    payload = '{"target_search": {"q": "Flüchtlinge"},"utility": "common_facet_values","utility_parameters": {"n": 5}}'.encode('utf-8')

    def setUp(self):
        self.headers, self.url = read_config(self.utype)
        url=os.path.join(self.url, "analysis/")
        self.response = requests.request("POST", url, data=self.payload, headers={'content-type': "application/json", **self.headers}).json()

        self.url = os.path.join(self.url, "report/")

