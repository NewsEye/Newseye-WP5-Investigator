from app.analysis.analysis_utils import AnalysisUtility


class ExtractWords(AnalysisUtility):
    def __init__(self):
        self.utility_name='extract_words'
        self.utility_description = 'collects doc->words information from a set of documents'
        self.utility_parameters=[]
        self.input_type='id_list'
        self.output_type='word_search'
        super(ExtractWords, self).__init__()
    
    async def __call__(self, task):
        """ Queries word index in the Solr to obtain documents split into words """
        



        

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


    
