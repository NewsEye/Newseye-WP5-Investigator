import assistant.config as conf

TOOL_LIST = {
    'extract_facets': lambda API, query: extract_facets(API, query),
    'common_topics': lambda API, query, n: common_topics(API, query, n),
}

TOOL_ARGS = {
    'extract_facets': [],
    'common_topics': ['n'],
}


def extract_facets(PSQLAPI, task):
    result = task['task_result']
    facets = {}
    for feature in result['included']:
        if feature['type'] != 'facet':
            continue
        values = []
        for item in feature['attributes']['items']:
            values.append((item['attributes']['label'], item['attributes']['hits']))
        facets[feature['id']] = values
    analysis_result = {
        'analysis_type': 'facet_counts',
        'analysis_result': facets
    }
    PSQLAPI.add_analysis(task['username'], task['task_id'], analysis_result)
    return analysis_result


def common_topics(PSQLAPI, query, n):
    facet_counts = PSQLAPI.get_analysis_by_query(query['task_id'], 'facet_counts')
    if facet_counts is None:
        facet_counts = extract_facets(PSQLAPI, query)
    facet_counts = facet_counts['analysis_result']
    topics = facet_counts[conf.TOPIC_FACET][:int(n)]
    analysis_result = {
        'analysis_type': 'common_topics',
        'analysis_result': topics
    }
    PSQLAPI.add_analysis(query['username'], query['task_id'], analysis_result)
    return analysis_result

