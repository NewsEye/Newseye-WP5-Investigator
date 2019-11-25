import unittest
import requests
from read_config import read_config
from time import sleep
import json
import os

class TestUtility(unittest.TestCase):
    utype="analysis"
    
    def setUp(self):
        self.headers, self.url = read_config(self.utype)
        self.response = requests.request("POST", self.url, data=self.payload, headers={'content-type': "application/json", **self.headers}).json()
    
    def test_query(self):
        print("RESPONSE %s" %self.response)
        self.assertIn("uuid", self.response, "Response has no uuid")

    def expected_result(self):
        with open(os.path.join(os.path.dirname(__file__), "task_result", self.task_result)) as js:
            return json.load(js, strict=False)

    def test_task_result(self, max_try=10):
        url = self.url + self.response["uuid"]
        for t in range(max_try+1):
            #print(t)
            response = requests.request("GET", url, data="", headers=self.headers)
            
            #print("RESPONSE:", response)
            response = response.json()
            #print("JSON:", response)
            #print(response.get("uuid", None))
            #print(response.get("task_status", None))
            if response.get("task_status", None) == 'running':
                print("sleep %d" %t**2)
                sleep(t**2)
            else:
                break
            
        returned_result = response.get('task_result', response)
        expected_result = self.expected_result()
        
        err = "Task takes too much time" if response.get("task_status", None)=="running" else "Unexpected task result"
        self.assertEquals(returned_result, expected_result, err)



class TestUtilityList(TestUtility):
    headers, url = read_config()
    url = os.path.join(url, "utilities/")
    task_result = "utility_list_task_result.json"

    def setUp(self):
        self.response = requests.request("GET", self.url, data="", headers=self.headers).json()
        
    def test_query(self):
        self.assertEquals(self.response, self.expected_result(), "Unexpected task result")
                          
    @unittest.skip("not available for this utility")
    def test_task_result(self):
        pass


class TestExtractDocID(TestUtility):
    task_result = "extract_docid_task_result.json"
    payload = '{"search_query": {"q": "sortiraient","qf" : "all_text_tfr_siv"},"utility": "extract_document_ids","force_refresh": "T"}'

class TestExtractWords(TestUtility):
    task_result = "extract_words_task_result.json"
    payload = '{"search_query": {"q": "sortiraient","qf" : "all_text_tfr_siv"},"utility" : "extract_words","force_refresh":"T"}'
   

class TestTfIdf(TestUtility):
    task_result = "tfidf_task_result.json"
    payload = '{"search_query": {"q": "sortiraient","qf" : "all_text_tfr_siv"},"utility" : "compute_tf_idf","force_refresh":"T"}'
   
class TestFindSteps(TestUtility):
    task_result = "find_steps_task_result.json"
    payload = '{"search_query": {"q": "Republik"},"utility": "find_steps_from_time_series","force_refresh": "T"}'

class TestExtractFacets(TestUtility):
    task_result = "extract_facets_task_result.json"
    payload = '{"search_query": {"q": "maito"},"utility": "extract_facets","force_refresh": "T"}'


class TestGenerateTimeseries(TestUtility):
    task_result = "generate_timeseries_task_result.json"
    payload = '{"search_query": {"q": "maito"},"utility": "generate_time_series","utility_parameters": {"facet_name": "NEWSPAPER_NAME"},"force_refresh": "T"}'
    
class TestCommonFacetValues(TestUtility):
    task_result = "common_facets_task_result.json"
    payload = '{"search_query": {"q": "maito"},"utility": "common_facet_values","n": 5,"force_refresh":"T"}'
           
    

    
    

    
