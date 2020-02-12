from test_utilities import TestUtility


class TestTopic(TestUtility):
    task_result = "topics_task_result.json"
    payload = '{"search_query": {"q": "Republik Flüchtlinge Australien","fq": "member_of_collection_ids_ssim:arbeiter_zeitung","mm": 3},"utility": "query_topic_model","utility_parameters": {"model_type": "lda","model_name": "arbeit-zeitung-lda"},"force_refresh": "T"}'.encode(
        "utf-8"
    )


class TestTopicLinking(TestUtility):
    task_result = "tm_doclinking_task_result.json"
    payload = (
        '{"search_query": {"q": "Republik"},"utility": "tm_document_linking", "force_refresh": "T"}'
    )
