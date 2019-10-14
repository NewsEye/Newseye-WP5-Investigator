from app.analysis.analysis_utils import AnalysisUtility
from flask import current_app
from app.search.search_utils import search_database
from collections import defaultdict
import pandas as pd
import numpy as np
from app.analysis import assessment
import json
import asyncio

def make_batches(number_of_items, batch_size = 100):
    number_of_batches = ((number_of_items-1)//batch_size)+1             
    start = 0
    for i in range(1,number_of_batches+1):
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
            },
            # TODO: language
]
        self.input_type='id_list'
        self.output_type='word_counts'
        super(ExtractWords, self).__init__()

    async def count(self, response, word2docid):
        for word_info in response['docs']:
            # TODO: search only required languages 
            word = [word_info[f] for f in ["text_tfr_siv","text_tse_siv","text_tde_siv","text_tfi_siv"] if f in word_info][0]
            docid = word_info['id'][:-9]# word id has a form docid_pageno_word_wordno, e.g. la_presse_12148-bpt6k477386c_3_word_1
            word2docid[word].append(docid)
        
    async def search(self, docids, word2docid):
        qs = [{"q" : docid + '*'} for docid in docids]
        responses = await search_database(qs, retrieve='words')
        await asyncio.gather(*[self.count(response, word2docid) for response in responses])
        
    async def call(self, task):
        """ Queries word index in the Solr to obtain document s split into words """
        min_count = int(task.utility_parameters.get('min_count'))
        word2docid = defaultdict(list)

        for (s,e) in make_batches(len(self.input_data)):
            await self.search(self.input_data[s:e], word2docid)
            
        # parallel
        # doesn't work due to "too many connections error"
        # await asyncio.gather(*[self.search(self.input_data[s:e], word2docid) for (s,e) in make_batches(len(self.input_data))])


        current_app.logger.debug("DOCS %d, WORDS %d" %(len(self.input_data), len(word2docid)))

        counts = {w:len(d) for w,d in word2docid.items() if len(d) >= min_count}
        current_app.logger.debug("ExtractWords: frequent words %d" %len(counts))
        
        total = sum(counts.values())
        relatives = {w:c/total for w,c in counts.items()}
        result = {"counts":counts, "relatives":relatives}
        return {'result':result,
                'interestingness':0}
        

class ComputeTfIdf(AnalysisUtility):
    def __init__(self):
        self.utility_name='compute_tf_idf'
        self.utility_description = 'computes TfIdf to compare given subcorpus to a whole corpus'
        # relies on min_count in extract_words utility
        self.utility_parameters  = [
            {
                'parameter_name': 'interest_thr',
                'parameter_description': 'Threshold for tfidf to be considered interesting. Value is interesting if value-mean > interest_thr*stabdard_deviation',
                'parameter_type': 'float',
                'parameter_default': 1,
                'parameter_is_required': False
            },
            {
                'parameter_name': 'languages',
                'parameter_description': 'use only documents written in this languages',
                'parameter_type' : 'string',
                'parameter_default' : None,
                'parameter_is_required' : False
            }
] 
        self.input_type = 'word_counts'
        self.output_type = 'tf_idf'
        super(ComputeTfIdf, self).__init__()


        
    async def call(self, task):
        "Gets word counts, query database for each word document frequency, than makes tf-idf statistics"
        interest_thr = task.utility_parameters.get('interest_thr')
        
        count, tf, df, N = await self.query_data(task)
        df = self.compute_td_idf(tf, df, N, interest_thr)
        df["count"] = [count[w] for w in df.index]
        df["ipm"] = df.tf*1e6

        return {'result' : json.loads(df[["count", "ipm", "tfidf"]].to_json(orient='index', double_precision=6)),
                'interestingness' : json.loads(df[df.interest>0]["interest"].to_json(orient='index'))}

      
    @staticmethod
    def compute_td_idf(tf, df, N, thr):
        # method might be useful later (e.g. for bigram tf-idf)
        df = pd.DataFrame.from_dict([{"word":w, "tf":tf[w], "df":df[w]} for w in df])
        df.set_index("word", inplace=True)
        df["tfidf"] = df.tf*np.log(N/df["df"])
        # TODO: more sophosticated elbow-based method
        df["interest"] = assessment.find_large_numbers_from_lists(df["tfidf"], coefficient=thr)
        return df.sort_values(by=['tfidf'], ascending=False)
    
    async def query_data(self, task):
        
        # TODO: parallel
        
        counts = self.input_data['counts']
        relatives = self.input_data['relatives']
        # qf means query field, the query field differes depending on a wanted language

        qf = task.search_query.get("qf", None)
        languages = task.utility_parameters.get('languages')
        if languages:
            lang_fields = ['all_text_t'+l+'_siv' for l in languages]
        elif qf:
            # for word search we don't need anything but language query field
            lang_fields = [langf for langf in ['all_text_tfr_siv', 'all_text_tfi_siv',
                                             'all_text_tde_siv', 'all_text_tse_siv'] if langf in qf]
       
        # FIND TOTAL
        # TODO: facet fields
        # not optimal
        total = await search_database({"rows":0,
                 "q":" ".join(["%s : [* TO *]" %langf for langf in lang_fields])})
        total = total['numFound']
           
        # FIND DF
        qf = ' '.join(lang_fields)
        word_list = list(counts.keys())
        df = {}
        await asyncio.gather(*[self.search(word_list[s:e], df, total, qf) for (s,e) in make_batches(len(word_list), batch_size=1000)])

        return counts, relatives, df, total

    async def search(self, words, df, total, qf):
            qs = [{"q":w, "rows":0} for w in words]
            if qf:
                qs = [{**q, "qf":qf} for q in qs]

            responses = await search_database(qs)

            # the query return 0 for stopwords
            # this cannot be true zero since the words were previously found in the same db
            # thus replace zero with all
            df.update({w:r['numFound'] if r['numFound'] else total for w,r in zip(words,responses)})
        
      

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
