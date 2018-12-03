TOOL_LIST = {
    'extract_facets': lambda state: extract_facets(state),
    'common_topics': lambda state, n: common_topics(state, n),
}

TOOL_ARGS = {
    'extract_facets': [],
    'common_topics': ['n'],
}


def extract_facets(current_state):
    result = current_state.query_results
    facets = {}
    for feature in result['included']:
        if feature['type'] != 'facet':
            continue
        values = []
        for item in feature['attributes']['items']:
            values.append((item['attributes']['label'], item['attributes']['hits']))
        facets[feature['id']] = values
    current_state.analysis_results['extract_facets'] = facets


def common_topics(current_state, n):
    facets = current_state.analysis_results.get('extract_facets')
    if facets is None:
        extract_facets(current_state)
        facets = current_state.analysis_results['extract_facets']
    topics = facets['subject_topic_facet'][:int(n)]
    current_state.analysis_results['common_topics'] = topics

