import assistant.config as conf
import asyncio
from operator import itemgetter
import pandas as pd


class AnalysisTools(object):

    def __init__(self, core):
        self._TOOL_LIST = {
            'extract_facets': lambda *args: self.extract_facets(*args),
            'common_topics': lambda *args: self.common_topics(*args),
            'topic_analysis': lambda *args: self.topic_analysis(*args),
            'split_document_set_by_facet': lambda *args: self.split_document_set_by_facet(*args),
        }

        self._core = core

    # TODO: The API for retrieving descriptions of available tools,
    async def async_analysis(self, username, tasks):

        async_tasks = [self._TOOL_LIST[task['task_parameters'].get('tool')](username, task) for task in tasks]

        results = await asyncio.gather(*async_tasks)
        print("Queries finished, returning results")
        return results

    async def extract_facets(self, username, task):
        facets = {}
        for feature in task['task_parameters']['data']['task_result']['included']:
            if feature['type'] != 'facet':
                continue
            values = []
            for item in feature['attributes']['items']:
                values.append((item['attributes']['label'], item['attributes']['hits']))
            facets[feature['id']] = values
        return facets

    async def common_topics(self, username, task):
        facet_counts = await self._core.execute_async_tasks(username, ('analysis', {'tool': 'extract_facets', 'target_query': task['task_parameters']['target_query']}))
        facet_counts = facet_counts[0]['task_result']
        topics = facet_counts[conf.AVAILABLE_FACETS['TOPIC']][:int(task['task_parameters']['n'])]
        return topics

    async def split_document_set_by_facet(self, username, task):

        # TODO: Fix the parent_id generation to implement the history properly

        split_facet = task['task_parameters'].get('split_facet', None)
        for item in task['task_parameters']['data']['task_result']['included']:
            if item['id'] == conf.AVAILABLE_FACETS[split_facet] and item['type'] == 'facet':
                facet_totals = [(facet['attributes']['value'], facet['attributes']['hits']) for facet in item['attributes']['items']]
                break
        else:
            raise TypeError("Query results don't contain required facet {}".format(conf.AVAILABLE_FACETS[split_facet]))
        facet_totals.sort()
        original_query = task['task_parameters']['target_query']
        queries = [{'f[{}][]'.format(conf.AVAILABLE_FACETS[split_facet]): item[0]} for item in facet_totals]
        for query in queries:
            query.update(original_query)
        query_ids = await self._core.execute_async_tasks(username, queries=queries, return_tasks=False)
        return [str(query_id) for query_id in query_ids]

    async def topic_analysis(self, username, task):
        if task['task_parameters']['data'] is None or task['task_parameters']['data']['task_status'] != 'finished':
            raise TypeError("No query results available for analysis")

        subquery_task = await self._core.execute_async_tasks(username, ('analysis', {'tool': 'split_document_set_by_facet', 'split_facet': 'PUB_YEAR', 'target_query': task['task_parameters']['target_query']}))
        subquery_ids = subquery_task[0]['task_result']

        subquery_results = self._core.get_results(subquery_ids).values()
        t_counts = []
        for task in subquery_results:
            query, data = itemgetter('task_parameters', 'task_result')(task)
            year = query['f[{}][]'.format(conf.AVAILABLE_FACETS['PUB_YEAR'])]
            total_hits = data['meta']['pages']['total_count']
            for item in data['included']:
                if item['id'] == conf.AVAILABLE_FACETS['TOPIC'] and item['type'] == 'facet':
                    t_counts.extend([[year, topic['attributes']['value'], topic['attributes']['hits'], topic['attributes']['hits'] / total_hits] for topic in item['attributes']['items']])
                    break
            else:
                raise TypeError("Query results don't contain required facet '{}'".format(conf.AVAILABLE_FACETS['TOPIC']))
        df = pd.DataFrame(t_counts, columns=['year', 'topic', 'count', 'rel_count'])
        abs_counts = df.pivot(index='topic', columns='year',values='count').fillna(0)
        rel_counts = df.pivot(index='topic', columns='year',values='rel_count').fillna(0)
        analysis_results = {
                'absolute_counts': abs_counts.to_dict(orient='index'),
                'relative_counts': rel_counts.to_dict(orient='index')
        }
        return analysis_results
