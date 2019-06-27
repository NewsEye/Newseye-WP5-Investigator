from app.analysis.analysis_utils import AnalysisUtility
from flask import current_app
from app.search.search_utils import search_database
from collections import defaultdict

def make_batches(number_of_items, batch_size = 100):
    number_of_batches = ((number_of_items-1)//batch_size)+1             
    start = 0
    for i in range(1,number_of_items+1):
        end = min(number_of_items, i*batch_size)
        yield(start, end)
        start = end



class ExtractWords(AnalysisUtility):
    def __init__(self):
        self.utility_name='extract_words'
        self.utility_description = 'collects doc->words information from a set of documents'
        self.utility_parameters = [
            {
                'parameter_name': 'min_count',
                'parameter_description': 'Minimal word count for word to be returned',
                'parameter_type': 'integer',
                'parameter_default': 20,
                'parameter_is_required': False
            }
        ]
        self.input_type='id_list'
        self.output_type='word_counts'
        super(ExtractWords, self).__init__()
        
    async def __call__(self, task):
        """ Queries word index in the Solr to obtain documents split into words """

        parameters = task.task_parameters.get('utility_parameters', {})
        min_count = int(parameters.get('min_count'))
        
        input_data = await self.get_input_data(task)
        input_data = input_data['result']

        word2docid = defaultdict(list)

        for (s,e) in make_batches(len(input_data)):
            docids = input_data[s:e]
            
            current_app.logger.debug("Search docs one by one")
            qs = [{"q" : docid + '*'} for docid in docids]
            responses = await search_database(qs, retrieve='words')

            current_app.logger.debug("search done, counting words")
            for docid, response in zip(docids, responses):
                for word_info in response['docs']:
                    word = [word_info[f] for f in ["text_tfr_siv", "text_tse_siv", "text_tde_siv", "text_tfi_siv"] if f in word_info][0]
                    word2docid[word].append(docid)
                   
        current_app.logger.debug("docs %d, words %d" %(len(input_data), len(word2docid)))
        counts = {w:len(d) for w,d in word2docid.items() if len(d) >= min_count}

        current_app.logger.debug("frequent words %d" %len(counts))
        
        total = sum(counts.values())
        relatives = {w:c*1e6/total for w,c in counts.items()}
        result = {"counts":counts, "ipms":relatives}
        return {'result':result,
                'interestingness':0}
        

class ComputeTfIdf(AnalysisUtility):
    def __init__(self):
        self.utility_name='compute_tf_idf'
        self.utility_description = 'computes TfIdf to compare given subcorpus to a whole corpus'
        # relies on min_count in extract_words utility
        self.utility_parameters  = []
        self.input_type = 'word_counts'
        self.output_type = 'compute_tf_idf'
        super(ComputeTfIdf, self).__init__()
        
    async def __call__(self, task):
        "Gets word counts, query database for each word document frequency, than makes tf-idf statistics"

        input_data = await self.get_input_data(task)
        counts = input_data['result']['counts']

        word_list = list(counts.keys())
        for (s,e) in make_batches(len(word_list), batch_size=1000):
            words = word_list[s:e]

            qs = [{"q":w, "rows":0} for w in words]
            responses = await search_database(qs)

            current_app.logger.debug(responses[0])
            break
        
        

     



        

class MakeBasicStats(AnalysisUtility):
    def __init__(self):
        self.utility_name = 'make_basic_stats'
        self.utility_description = 'Computes basic statistics for a given corpus: word counts, etc.'
        self.utility_parameters=[]
        self.input_type='word_search'
        self.output_type='stats'
        super(MakeBasicStats, self).__init__()

    async def __call__(self, task):
        raise NotImplementedError


    
        
        
        
 
class ExtractBigrams(AnalysisUtility):
    def __init__(self):
        self.utility_name='extract_words'
        self.utility_description = 'collects doc->words information from a set of documents'
        self.utility_parameters=[]
        self.input_type='id_list'
        self.output_type='word_search'
        super(ExtractBigrams, self).__init__()

        # similar to extract words but will need to take page information into account
        
        raise NotImplementedError
