import assistant.config as conf
import asyncio
from operator import itemgetter
import pandas as pd


class AnalysisTools(object):

    def __init__(self, core, database):
        self._TOOL_LIST = {
            'extract_facets': lambda *args: self.extract_facets(*args),
            'common_topics': lambda *args: self.common_topics(*args),
            'topic_analysis': lambda *args: self.topic_analysis(*args),
        }

        self._db = database

        self._core = core


    # TODO: The API for retrieving descriptions of available tools,
    async def async_query(self, username, queries):
        target_ids = [query.get('target_id') for query in queries]
        # Todo: get rid of the direct database access in here. Perhaps pass the target_results as a parameter from the core?
        target_results = self._db.get_results_by_task_id(target_ids)

        tasks = []

        for query, target_id in zip(queries, target_ids):
            tool_name = query.get('tool')
            tasks.append(self._TOOL_LIST[tool_name](username, query, target_results[target_id]))

        results = await asyncio.gather(*tasks)
        print("Queries finished, returning results")
        return results

    async def extract_facets(self, username, query, data):
        facets = {}
        for feature in data['task_result']['included']:
            if feature['type'] != 'facet':
                continue
            values = []
            for item in feature['attributes']['items']:
                values.append((item['attributes']['label'], item['attributes']['hits']))
            facets[feature['id']] = values
        return facets

    async def common_topics(self, username, query, data):
        facet_counts = await self._core.execute_async_tasks(username, ('analysis', {'tool': 'extract_facets', 'target_id': query['target_id']}))
        facet_counts = facet_counts[0]['task_result']
        topics = facet_counts[conf.TOPIC_FACET][:int(query['n'])]
        return topics

    async def topic_analysis(self, username, query, data):
        if data is None or data['task_result'] == conf.UNFINISHED_TASK_RESULT:
            raise TypeError("No query results available for analysis")
        for item in data['task_result']['included']:
            if item['id'] == conf.PUB_YEAR_FACET and item['type'] == 'facet':
                pub_dates = [(date['attributes']['value'], date['attributes']['hits']) for date in item['attributes']['items']]
                break
        else:
            raise TypeError("Query results don't contain required facet {}".format(conf.PUB_YEAR_FACET))
        pub_dates.sort()
        last_query = data['task_parameters']
        queries = [{'f[{}][]'.format(conf.PUB_YEAR_FACET): item[0]} for item in pub_dates]
        for query in queries:
            query.update(last_query)
        results = await self._core.execute_async_tasks(username, queries)
        t_counts = []
        for task in results:
            query, data = itemgetter('task_parameters', 'task_result')(task)
            year = query['f[{}][]'.format(conf.PUB_YEAR_FACET)]
            total_hits = data['meta']['pages']['total_count']
            for item in data['included']:
                if item['id'] == conf.TOPIC_FACET and item['type'] == 'facet':
                    t_counts.extend([[year, topic['attributes']['value'], topic['attributes']['hits'], topic['attributes']['hits'] / total_hits] for topic in item['attributes']['items']])
                    break
            else:
                raise TypeError("Query results don't contain required facet '{}'".format(conf.TOPIC_FACET))
        df = pd.DataFrame(t_counts, columns=['year', 'topic', 'count', 'rel_count'])
        abs_counts = df.pivot(index='topic', columns='year',values='count').fillna(0)
        rel_counts = df.pivot(index='topic', columns='year',values='rel_count').fillna(0)
        analysis_results = {
                'absolute_counts': abs_counts.to_dict(orient='index'),
                'relative_counts': rel_counts.to_dict(orient='index')
        }
        return analysis_results
