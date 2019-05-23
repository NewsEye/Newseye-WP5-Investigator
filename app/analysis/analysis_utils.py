from app import db
from app.models import Task
from app.search.search_utils import search_database
from config import Config
from flask import current_app
import asyncio
import numpy as np
import pandas as pd
import requests
from app.analysis import assessment, timeseries
from app.analysis.step_detection import FindStepsFromTimeSeries
from operator import itemgetter
import random
from main.db_utils import load_corpus_from_pickle


class AnalysisUtility(object):
    def __init__(self):
        self.utility_name = None
        self.utility_description = None
        self.utility_parameters = None
        self.input_type = None
        self.output_type = None

    async def __call__(self, task):
        return {
            'error': 'This utility has not yet been implemented'
        }

    @staticmethod
    def get_input_task(task):
        input_task_uuid = task.task_parameters.get('target_uuid')
        if input_task_uuid:
            input_task = Task.query.filter_by(uuid=input_task_uuid).first()
            if input_task is None:
                raise ValueError('Invalid target_uuid')
        else:
            input_task = None
        return input_task

    def get_description(self):
        return {
            'utility_name': self.utility_name,
            'utility_description': self.utility_description,
            'utility_parameters': self.utility_parameters,
            'input_type': self.input_type,
            'output_type': self.output_type
        }


class ExtractFacets(AnalysisUtility):
    def __init__(self):
        super(ExtractFacets, self).__init__()
        self.utility_name = 'extract_facets'
        self.utility_description = 'Examines the document set given as input, and finds all the different facets for which values have been set in at least some of the documents.'
        self.utility_parameters = []
        self.input_type = 'search_result'
        self.output_type = 'facet_list'

    async def __call__(self, task):
        """ Extract all facet values found in the input data and the number of occurrences for each."""
        input_task = self.get_input_task(task)
        task.hist_parent_id = input_task.uuid
        db.session.commit()
        input_data = input_task.task_result.result
        facets = {}
        for feature in input_data[Config.FACETS_KEY]:
            if Config.DATABASE_IN_USE == 'demo':
                if feature['type'] != 'facet':
                    continue
                values = {}
                for item in feature[Config.FACET_ATTRIBUTES_KEY][Config.FACET_ITEMS_KEY]:
                    values[item[Config.FACET_ATTRIBUTES_KEY][Config.FACET_VALUE_LABEL_KEY]] = \
                        item[Config.FACET_ATTRIBUTES_KEY][Config.FACET_VALUE_HITS_KEY]
                facets[feature[Config.FACET_ID_KEY]] = values
            elif Config.DATABASE_IN_USE == 'newseye':
                values = {}
                for item in feature[Config.FACET_ITEMS_KEY]:
                    values[item[Config.FACET_VALUE_LABEL_KEY]] = item[Config.FACET_VALUE_HITS_KEY]
                facets[feature[Config.FACET_ID_KEY]] = values
        return facets


class CommonFacetValues(AnalysisUtility):
    def __init__(self):
        super(CommonFacetValues, self).__init__()
        self.utility_name = 'common_facet_values'
        self.utility_description = 'Sorts the facet values for facet_name by decreasing number of matching documents and returns the n most common facet values and their document counts'
        self.utility_parameters = [
            {
                'parameter_name': 'n',
                'parameter_description': 'The number of facet values to be included in the result',
                'parameter_type': 'integer',
                'parameter_default': 5,
                'parameter_is_required': False,
            },
            {
                'parameter_name': 'facet_name',
                'parameter_description': 'The name of the facet to be analysed, e.g. PUB_YEAR',
                'parameter_type': 'string',
                'parameter_default': None,
                'parameter_is_required': True
            }
        ]
        self.input_type = 'facet_list'
        self.output_type = 'topic_list'

    async def __call__(self, task):
        default_parameters = {
            'n': 5,
            'facet_name': 'TOPIC',
        }
        n = int(task.task_parameters.get('n', default_parameters['n']))
        facet_name = task.task_parameters.get('facet_name', default_parameters['facet_name'])
        facet_name = Config.AVAILABLE_FACETS.get(facet_name, facet_name)

        input_task = self.get_input_task(task)
        task.hist_parent_id = input_task.uuid
        db.session.commit()

        input_data = input_task.task_result.result
        facets = input_data[facet_name]
        facet_list = [(facets[key], key) for key in facets.keys()]
        facet_list.sort(reverse=True)
        facet_list = facet_list[:n]
        facet_list = [{"facet_value": key, "document_count": value} for value, key in facet_list]
        interestingness = [1] * len(facet_list)
        return {'facet_counts': facet_list, 'interestingness': interestingness}


class GenerateTimeSeries(AnalysisUtility):
    def __init__(self):
        super(GenerateTimeSeries, self).__init__()
        self.utility_name = 'generate_time_series'
        self.utility_description = ''
        self.utility_parameters = [
            {
                'parameter_name': 'facet_name',
                'parameter_description': 'Not yet written',
                'parameter_type': 'string',
                'parameter_default': None,
                'parameter_is_required': True
            }
        ]
        self.input_type = 'search'
        self.output_type = 'time_series'

    async def __call__(self, query):
        # TODO Add support for total document count

        input_task = self.get_input_task(query)
        input_data = input_task.task_result.result

        facet_name = query.task_parameters.get('facet_name')
        facet_string = Config.AVAILABLE_FACETS.get(facet_name)
        if facet_string is None:
            raise TypeError("Facet not specified or specified facet not available in current database")

        year_facet = Config.AVAILABLE_FACETS['PUB_YEAR']
        for facet in input_data[Config.FACETS_KEY]:
            if facet[Config.FACET_ID_KEY] == year_facet:
                facet_values = [item['value'] for item in facet['items']]
                break
        else:
            raise TypeError(
                "Search results don't contain required facet {}".format(year_facet))
        year_parameter_names = ['f[{}][]'.format(year_facet), 'range[{}][begin]'.format(year_facet), 'range[{}][end]'.format(year_facet)]
        original_search = {key: value for key, value in input_task.task_parameters.items() if key not in year_parameter_names}
        queries = [{'f[{}][]'.format(year_facet): item} for item in facet_values]
        for query in queries:
            query.update(original_search)
        query_results = await search_database(queries)
        f_counts = []
        for query, result in zip(queries, query_results):
            if result is None:
                current_app.logger.error('Empty query result in generate_time_series')
                continue
            year = query['f[{}][]'.format(year_facet)]
            total_hits = result['pages']['total_count']
            for facet in result[Config.FACETS_KEY]:
                if facet[Config.FACET_ID_KEY] == facet_string:
                    f_counts.extend([[year, item['value'], item['hits'], item['hits'] / total_hits]
                                     for item in facet['items']])
                    break
            # TODO: count the number of items with no value defined for the desired facet
        df = pd.DataFrame(f_counts, columns=['year', facet_name, 'count', 'rel_count'])
        abs_counts = df.pivot(index=facet_name, columns='year', values='count').fillna(0)
        rel_counts = df.pivot(index=facet_name, columns='year', values='rel_count').fillna(0)
        analysis_results = {
            'absolute_counts': abs_counts.to_dict(orient='index'),
            'relative_counts': rel_counts.to_dict(orient='index')
        }
        return analysis_results


class CompareDocumentSets(AnalysisUtility):
    async def __call__(self, task):
        input_task = self.get_input_task(task)
        input_data = input_task.task_result.result


class WordCount(AnalysisUtility):
    async def __call__(self, task):
        input_task = self.get_input_task(task)
        input_data = input_task.task_result.result
        word_counts = self.do_magic(input_data)
        return word_counts

    @staticmethod
    def do_magic(data):
        return {'Stuff': 1}


class ExtractDocumentIds(AnalysisUtility):
    def __init__(self):
        super(ExtractDocumentIds, self).__init__()
        self.utility_name = 'extract_document_ids'
        self.utility_description = 'Examines the document set given as input, and extracts the document_ids for each of the documents.'
        self.utility_parameters = []
        self.input_type = 'search_result'
        self.output_type = 'id_list'

    async def __call__(self, task):
        demo_documents = int(task.task_parameters.get('demo_mode', None))
        if demo_documents:
            return [random.randint(0, 9458) for i in range(demo_documents)]
        else:
            input_task = self.get_input_task(task)
            task.hist_parent_id = input_task.uuid
            db.session.commit()
            input_data = input_task.task_result.result
            document_ids = [item['id'] for item in input_data['data']]
            return document_ids


class QueryTopicModel(AnalysisUtility):
    def __init__(self):
        super(QueryTopicModel, self).__init__()
        self.utility_name = 'query_topic_model'
        self.utility_description = 'Queries the selected topic model.'
        self.utility_parameters = [
            {
                'parameter_name': 'model_type',
                'parameter_description': 'The type of the topic model to use',
                'parameter_type': 'string',
                'parameter_default': None,
                'parameter_is_required': True,
            },
            {
                'parameter_name': 'model_name',
                'parameter_description': 'The name of the topic model to use',
                'parameter_type': 'string',
                'parameter_default': None,
                'parameter_is_required': False,
            },
        ]
        self.input_type = 'id_list'
        self.output_type = 'topic_analysis'

    async def __call__(self, task):
        model_type = task.task_parameters.get('model_type')
        if model_type is None:
            raise KeyError
        model_name = task.task_parameters.get('model_name')
        if model_name is None:
            available_models = self.request_topic_models(model_type)
            model_name = available_models[0]['name']
        input_task = self.get_input_task(task)
        db.session.commit()
        payload = {
            'model': model_name,
            'documents': input_task.task_result.result
        }
        response = requests.post('{}/{}/query'.format(Config.TOPIC_MODEL_URI, model_type), json=payload)
        uuid = response.json().get('task_uuid')
        if not uuid:
            raise ValueError('Invalid response from the Topic Model API')
        delay = 60
        while delay < 300:
            await asyncio.sleep(delay)
            delay *= 1.5
            response = requests.post('{}/query-results'.format(Config.TOPIC_MODEL_URI), json={'task_uuid': uuid})
            if response.status_code == 200:
                break
        return response.json()

    @staticmethod
    def request_topic_models(model_type):
        response = requests.get('{}/{}/list-models'.format(Config.TOPIC_MODEL_URI, model_type))
        return response.json()


class LemmaFrequencyTimeseries(AnalysisUtility):
    def __init__(self):
        super(LemmaFrequencyTimeseries, self).__init__()
        self.utility_name = 'lemma_frequency_timeseries'
        self.utility_description = ''
        self.utility_parameters = [
            {
                'parameter_name': 'corpus_filename',
                'parameter_description': 'File where the corpus is pickled',
                'parameter_type': 'string',
                'parameter_default': None,
                'parameter_is_required': True
            },
            {
                'parameter_name': 'word',
                'parameter_description': 'Word to be analysed',
                'parameter_type': 'string',
                'parameter_default': None,
                'parameter_is_required': True
            },
            {
                'parameter_name': 'word_type',
                'parameter_description': "'lemma' or 'token",
                'parameter_type': 'string',
                'parameter_default': None,
                'parameter_is_required': True
            }
        ]
        self.input_type = 'corpus'
        self.output_type = 'timeseries'

    async def __call__(self, task):
        filename, word, word_type = itemgetter('corpus_filename', 'word', 'word_type')(task.task_parameters)
        corpus = load_corpus_from_pickle(filename)
        if corpus is None:
            return None
        ts, ts_ipm = corpus.timeseries(word_type, 'year', word_list=[word])

        return {'absolute_counts': ts, 'relative_counts': ts_ipm}


class AnalyseLemmaFrequency(AnalysisUtility):
    def __init__(self):
        super(AnalyseLemmaFrequency, self).__init__()
        self.utility_name = 'analyse_lemma_frequency'
        self.utility_description = ''
        self.utility_parameters = [
            {
                'parameter_name': 'corpus_filename',
                'parameter_description': 'File where the corpus is pickled',
                'parameter_type': 'string',
                'parameter_default': None,
                'parameter_is_required': True
            },
            {
                'parameter_name': 'word',
                'parameter_description': 'Word to be analysed',
                'parameter_type': 'string',
                'parameter_default': None,
                'parameter_is_required': True
            },
            {
                'parameter_name': 'suffix',
                'parameter_description': 'Suffix used for comparison',
                'parameter_type': 'string',
                'parameter_default': None,
                'parameter_is_required': True
            }
        ]
        self.input_type = None
        self.output_type = 'something_cool'

    async def __call__(self, task):
        filename, word, suffix = itemgetter('corpus_filename', 'word', 'suffix')(task.task_parameters)
        corpus = load_corpus_from_pickle(filename)
        if corpus is None:
            return None
        group = corpus.find_lemmas_by_suffix(suffix)
        counts = {w: len(corpus.lemma_to_docids[w]) for w in group}
        print("Words with suffix '%s', sorted by count:" % suffix)
        for (w, c) in sorted(counts.items(), key=lambda x: x[1], reverse=True):
            print(w, c)

        wfr = timeseries.compare_word_to_group(corpus, word, group)

        ts, ts_ipm = corpus.timeseries('lemma', 'month')

        group_ts = timeseries.sum_up({w: ts[w] for w in group})
        group_ts_ipm = timeseries.sum_up({w: ts_ipm[w] for w in group})

        print("'%s': averaged count %3.2f, averaged relative count (ipm) %3.2f" %
              (word, np.mean(list(ts[word].values())), np.mean(list(ts_ipm[word].values()))))
        print("'%s': averaged count %3.2f, averaged relative count (ipm) %3.2f" %
              (suffix, np.mean(list(group_ts.values())), np.mean(list(group_ts_ipm.values()))))

        spikes = assessment.find_large_numbers(wfr)
        print("Potentially interesting dates:")
        for k in sorted(spikes, key=spikes.get, reverse=True):
            print("%s: '%s': %d (%2.2f ipm), '%s': %d (%2.2f ipm)" %
                  (k, word, ts[word][k], ts_ipm[word][k], suffix, group_ts[k], group_ts_ipm[k]))
        return {'wfr': wfr, 'ts': ts, 'ts_ipm': ts_ipm, 'spikes': spikes}


UTILITY_MAP = {
    'extract_facets': ExtractFacets(),
    'common_facet_values': CommonFacetValues(),
    'generate_time_series': GenerateTimeSeries(),
    'find_steps_from_time_series': FindStepsFromTimeSeries(),
    'extract_document_ids': ExtractDocumentIds(),
    'query_topic_model': QueryTopicModel(),
    'lemma_frequency_timeseries': LemmaFrequencyTimeseries(),
    'analyse_lemma_frequency': AnalyseLemmaFrequency(),
}
