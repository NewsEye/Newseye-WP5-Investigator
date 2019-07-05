import requests
import json
import os, sys
import jwt, datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from read_config import read_config
from time import sleep

def make_response(url, payload, headers, max_try):
    response = requests.request("POST", url, data=payload, headers={'content-type': "application/json", **headers}).json()
    url = url + response["uuid"]   
    
    for t in range(max_try+1):
        response = requests.request("GET", url, data="", headers=headers).json()
        if response['task_status'] == 'running':
            sleep(t**2)
        else:
            break

    return response['task_result']

def make_report_response(payload, max_try):
    # max_try is not used
    # if task execution is too slow may need to do it in a sleeping loop
    
    headers, url = read_config('report')
    response = requests.request("POST", os.path.join(url, "analysis/"),
                                data=payload, headers={'content-type': "application/json", **headers}).json()
    return requests.request("GET", os.path.join(url, "report/", response["uuid"]), data="", headers=headers).json()
    

def make_analysis_response(payload, max_try):
    headers, url = read_config()
    return make_response(url, payload, headers, max_try=max_try)



def make_test_response(utility_name, max_try = 10): 
    
    if utility_name == 'search':
        headers, url = read_config(utility_name)
        payload = '{"q": "Republik AND Flüchtlinge AND Australien","fq": "member_of_collection_ids_ssim:arbeiter_zeitung","mm": 3,"force_refresh":"T"}'.encode('utf-8')
        task_result = make_response(url, payload, headers, max_try=max_try)
        
    elif utility_name == 'topics':
        payload = '{"target_search": {"q": "Republik Flüchtlinge Australien","fq": "member_of_collection_ids_ssim:arbeiter_zeitung","mm": 3},"utility": "query_topic_model","utility_parameters": {"model_type": "lda","model_name": "arbeit-zeitung-lda"},"force_refresh": "T"}'.encode('utf-8')
        task_result = make_analysis_response(payload, max_try=max_try)

    elif utility_name == 'utility_list':
        headers, url = read_config()
        url = os.path.join(url, "utilities/")
        task_result = requests.request("GET", url, data="", headers=headers).json()

    elif utility_name == 'extract_docid':
        payload = '{"target_search": {"q": "sortiraient","qf" : "all_text_tfr_siv"},"utility": "extract_document_ids","force_refresh": "T"}'
        task_result = make_analysis_response(payload, max_try=max_try)

    elif utility_name == 'extract_words':
        payload = '{"target_search": {"q": "sortiraient","qf" : "all_text_tfr_siv"},"utility" : "extract_words"}'
        task_result = make_analysis_response(payload, max_try=max_try)    
        
    elif utility_name == 'tfidf':
        payload = '{"target_search": {"q": "sortiraient","qf" : "all_text_tfr_siv"},"utility" : "compute_tf_idf"}'
        task_result = make_analysis_response(payload, max_try=max_try)

    elif utility_name == 'extract_facets':
        payload = '{"target_search": {"q": "maito"},"utility": "extract_facets","force_refresh": "T"}'
        task_result = make_analysis_response(payload, max_try=max_try)

    elif utility_name == 'generate_timeseries':
        payload = '{"target_search": {"q": "maito"},"utility": "generate_time_series","utility_parameters": {"facet_name": "NEWSPAPER_NAME"},"force_refresh": "True"}'
        task_result = make_analysis_response(payload, max_try=max_try)

    elif utility_name == 'common_facets':
        payload = '{"target_search": {"q": "maito"},"utility": "common_facet_values","n": 5}'
        task_result = make_analysis_response(payload, max_try=max_try)

    elif utility_name == 'find_steps':
        payload = '{"target_search": {"q": "Republik"},"utility": "find_steps_from_time_series","force_refresh": "True"}'
        task_result = make_analysis_response(payload, max_try=max_try)

    elif utility_name == 'report':
        payload = '{"target_search": {"q": "Flüchtlinge"},"utility": "common_facet_values","utility_parameters": {"n": 5}}'.encode('utf-8')
        task_result = make_report_response(payload, max_try=max_try)

    os.chdir(os.path.abspath(os.path.dirname(__file__)))
    outfile_name = utility_name + "_task_result.NEW.json"
    with open(outfile_name, 'w') as outfile:  
        json.dump(task_result, outfile)
    print("Task result written into %s" %outfile_name)


if __name__ == '__main__':
    try:
        make_test_response(sys.argv[1])
    except Exception as e:
        print("USAGE: make_task_response.py [search|utility_list|topics|tfidf|extract_docid|extract_facets|generate_timeseries|extract_words|find_steps|common_facets|report]")
        # raise e
