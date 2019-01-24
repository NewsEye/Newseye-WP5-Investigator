import assistant.config as conf
import asyncio


class AnalysisTools(object):

    def __init__(self, core, database):
        self._TOOL_LIST = {
            'extract_facets': lambda *args: self.extract_facets(*args),
            'common_topics': lambda *args: self.common_topics(*args),
        }

        self._db = database

        self._core = core

    # TODO: 1) retrieve the results using the target_ids for all of the queries in a single psql call
    #          (use a dict {target_id: result})
    #       2) generate the async tasks and run them in parallel
    #       3) return all the analysis results (core takes care of updating them to the psql database)

    async def async_query(self, username, queries):
        target_ids = [query.get('target_id') for query in queries]
        target_results = self._db.get_results_by_id(target_ids)  # {target_id: (target_id, result)}

        tasks = []

        for query, target_id in zip(queries, target_ids):
            tool_name = query.get('tool')
            tasks.append(self._TOOL_LIST[tool_name](username, query, target_results[target_id]))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        print("Queries finished, returning results")
        return results

    async def extract_facets(self, username, query, data):
        facets = {}
        for feature in data[1]['included']:
            if feature['type'] != 'facet':
                continue
            values = []
            for item in feature['attributes']['items']:
                values.append((item['attributes']['label'], item['attributes']['hits']))
            facets[feature['id']] = values
        return facets

    async def common_topics(self, username, query, data):
        # ToDO: Check that this works
        facet_counts = self._core.run_query_task(username, ('analysis', {'tool': 'extract_facets', 'target_id': query['target_id']}), threaded=False)[0]
        facet_counts = facet_counts['task_result']
        topics = facet_counts[conf.TOPIC_FACET][:int(query['n'])]
        return topics

