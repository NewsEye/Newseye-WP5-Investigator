from app import db
from app.models import Task
from app.search.search_utils import search_database
from config import Config
from flask import current_app
import numpy as np
import pandas as pd
from app.analysis import assessment, timeseries
from operator import itemgetter
import random
from app.main.db_utils import load_corpus_from_pickle
from werkzeug.exceptions import BadRequest


class AnalysisUtility(object):
    def __init__(self):
        self.parameter_defaults = {}
        self.set_defaults()

    async def __call__(self, task):
        return {
            'error': 'This utility has not yet been implemented'
        }

    @staticmethod
    def get_input_task(task):
        input_task_uuid = task.task_parameters.get('target_uuid', None)
        if input_task_uuid:
            input_task = Task.query.filter_by(uuid=input_task_uuid).first()
        else:
            input_task = None
        return input_task

    def set_defaults(self):
        if self.utility_parameters:
            self.parameter_defaults = {param['parameter_name']: param['parameter_default'] for param in self.utility_parameters}

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
        self.utility_name = 'extract_facets'
        self.utility_description = 'Examines the document set given as input, and finds all the different facets for which values have been set in at least some of the documents.'
        self.utility_parameters = []
        self.input_type = 'search_query'
        self.output_type = 'facet_list'
        super(ExtractFacets, self).__init__()

    async def __call__(self, task):
        """ Extract all facet values found in the input data and the number of occurrences for each."""
        input_task = self.get_input_task(task)
        if input_task:
            task.hist_parent_id = input_task.uuid
            db.session.commit()
            input_data = input_task.task_result.result
        elif task.task_parameters.get('target_search'):
            input_data = await search_database(task.task_parameters['target_search'], database='solr', retrieve='facets')
        else:
            raise BadRequest('Request missing valid target_uuid or target_search!')
        facets = {}
        for feature in input_data[Config.FACETS_KEY]:
            values = {}
            for item in feature[Config.FACET_ITEMS_KEY]:
                values[item[Config.FACET_VALUE_LABEL_KEY]] = item[Config.FACET_VALUE_HITS_KEY]
            facets[feature[Config.FACET_ID_KEY]] = values
        return {'result': facets,
                'interestingness': facets}


class CommonFacetValues(AnalysisUtility):
    def __init__(self):
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
                'parameter_default': 'PUB_YEAR',
                'parameter_is_required': False
            }
        ]
        self.input_type = 'facet_list'
        self.output_type = 'topic_list'
        super(CommonFacetValues, self).__init__()

    async def __call__(self, task):
        parameters = task.task_parameters['utility_parameters']
        n = int(parameters.get('n', self.parameter_defaults['n']))
        facet_name = parameters.get('facet_name', self.parameter_defaults['facet_name'])
        facet_name = Config.AVAILABLE_FACETS.get(facet_name, facet_name)

        input_task = self.get_input_task(task)
        task.hist_parent_id = input_task.uuid
        db.session.commit()

        input_data = input_task.task_result.result['result']
        facets = input_data[facet_name]
        facet_list = [(facets[key], key) for key in facets.keys()]
        facet_list.sort(reverse=True)
        facet_list = facet_list[:n]
        facet_list = [{"facet_value": key, "document_count": value} for value, key in facet_list]
        interestingness = [1] * len(facet_list)
        return {'result': facet_list,
                'interestingness': interestingness}


class GenerateTimeSeries(AnalysisUtility):
    def __init__(self):
        self.utility_name = 'generate_time_series'
        self.utility_description = ''
        self.utility_parameters = [
            {
                'parameter_name': 'facet_name',
                'parameter_description': 'the facet to be analysed',
                'parameter_type': 'string',
                'parameter_default': 'NEWSPAPER_NAME',
                'parameter_is_required': False
            },
            ## TODO: Add a parameter for choosing what to do with missing data
        ]
        self.input_type = 'search_result'
        self.output_type = 'time_series'
        super(GenerateTimeSeries, self).__init__()

    async def __call__(self, task):
        # TODO Add support for total document count

        parameters = task.task_parameters['utility_parameters']
        facet_name = parameters.get('facet_name')
        facet_string = Config.AVAILABLE_FACETS.get(facet_name)
        if facet_string is None:
            raise TypeError("Facet not specified or specified facet not available in current database")

        input_task = self.get_input_task(task)
        if input_task:
            input_data = input_task.task_result.result
        elif task.task_parameters.get('target_search'):
            input_data = await search_database(task.task_parameters['target_search'], database='solr', retrieve='facets')
        else:
            raise BadRequest('Request missing valid target_uuid or target_search!')

        year_facet = Config.AVAILABLE_FACETS['PUB_YEAR']
        for facet in input_data[Config.FACETS_KEY]:
            if facet[Config.FACET_ID_KEY] == year_facet:
                years_in_data = [item['value'] for item in facet['items']]
                break
        else:
            raise TypeError(
                "Search results don't contain required facet {}".format(year_facet))
        original_search = {key: value for key, value in input_task.task_parameters.items() if key != 'fq'}
        queries = [{'fq': '{}:{}'.format(year_facet, item)} for item in years_in_data]
        for query in queries:
            query.update(original_search)
        query_results = await search_database(queries, database='solr', retrieve='facets')
        f_counts = []
        for query, result in zip(queries, query_results):
            if result is None:
                current_app.logger.error('Empty query result in generate_time_series')
                continue
            _, year = query['fq'].split(':')
            total_hits = result['numFound']
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
        return {'result': analysis_results,
                'interestingness': 0}


class ExtractDocumentIds(AnalysisUtility):
    def __init__(self):
        self.utility_name = 'extract_document_ids'
        self.utility_description = 'Examines the document set given as input, and extracts the document_ids for each of the documents.'
        self.utility_parameters = []
        self.input_type = 'search_query'
        self.output_type = 'id_list'
        super(ExtractDocumentIds, self).__init__()

    async def __call__(self, task):
        parameters = task.task_parameters['utility_parameters']
        demo_documents = parameters.get('demo_mode', None)
        if demo_documents:
            return [random.randint(0, 9458) for i in range(int(demo_documents))]
        else:
            input_task = self.get_input_task(task)
            if input_task:
                task.hist_parent_id = input_task.uuid
                db.session.commit()
                input_data = input_task.task_result.result
            else:
                input_data = await search_database(task.task_parameters['target_search'], database='solr', retrieve='docids')
            document_ids = [item['id'] for item in input_data[Config.DOCUMENTS_KEY]]
            return {'result': document_ids,
                    'interestingness': 0}


class LemmaFrequencyTimeseries(AnalysisUtility):
    def __init__(self):
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
                'parameter_name': 'item',
                'parameter_description': 'The token or lemma to be analysed',
                'parameter_type': 'string',
                'parameter_default': None,
                'parameter_is_required': True
            },
            {
                'parameter_name': 'item_type',
                'parameter_description': "The type of the item to be analysed. Valid values are 'token-1' and 'lemma-1' for single words and 'token-2' and 'lemma-2' for bigrams.",
                'parameter_type': 'string',
                'parameter_default': 'token-1',
                'parameter_is_required': True
            }
        ]
        self.input_type = 'corpus'
        self.output_type = 'timeseries'
        super(LemmaFrequencyTimeseries, self).__init__()

    async def __call__(self, task):
        parameters = task.task_parameters['utility_parameters']
        filename, item, item_type = itemgetter('corpus_filename', 'item', 'item_type')(parameters)
        corpus = load_corpus_from_pickle(filename)
        if corpus is None:
            return None
        ts, ts_ipm = corpus.timeseries(item=item_type, granularity='year', word_list=[item])

        return {'result': {'absolute_counts': ts, 'relative_counts': ts_ipm},
                'interestingness': 0}


class AnalyseLemmaFrequency(AnalysisUtility):
    def __init__(self):
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
        self.input_type = 'corpus'
        self.output_type = 'word_frequency_statistics'
        super(AnalyseLemmaFrequency, self).__init__()

    async def __call__(self, task):
        parameters = task.task_parameters['utility_parameters']
        filename, word, suffix = itemgetter('corpus_filename', 'word', 'suffix')(parameters)
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
        return {'result': {'wfr': wfr, 'ts': ts, 'ts_ipm': ts_ipm, 'spikes': spikes},
                'interestingness': 0}
