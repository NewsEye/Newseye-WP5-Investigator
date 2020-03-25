from test_utilities import TestUtility
import requests
from read_config import read_config
import json
import os


class TestComparison(TestUtility):
    utype = "analysis"
    task_result = "comparison_task_result.json"

    payload1 = '{"search_query": {"q": "sortiraient","qf" : "all_text_tfr_siv"}, "utility": "common_facet_values","force_refresh":"T"}'
    payload2 = '{"search_query": {"q": "la_presse_12148-bpt6k5148665 la_presse_12148-bpt6k5440774 la_presse_12148-bpt6k478627w la_presse_12148-bpt6k479480z la_presse_12148-bpt6k4783044 la_presse_12148-bpt6k4779778 la_presse_12148-bpt6k543650c la_presse_12148-bpt6k5131113 la_presse_12148-bpt6k513085q la_presse_12148-bpt6k515326h", "mm": 1, "qf": "id all_text_tfr_siv"}, "utility": "common_facet_values","force_refresh":"T"}'

    headers, url = read_config()
    response1 = requests.request(
        "POST",
        url=url,
        data=payload1,
        headers={"content-type": "application/json", **headers},
    )
    response2 = requests.request(
        "POST",
        url=url,
        data=payload2,
        headers={"content-type": "application/json", **headers},
    )
    responses = [response1, response2]

    # this doesn't work for some reason
    # responses = [requests.request("POST", url=url, data=payload, headers={'content-type': "application/json", **headers}) for payload in [payload1, payload2]]

    responses = [response.json() for response in responses]
    uuids = [response["uuid"] for response in responses]

    payload = json.dumps(
        {
            "utility": "comparison",
            "utility_parameters": {"task_uuids": uuids},
            "force_refresh": "T",
        }
    )
