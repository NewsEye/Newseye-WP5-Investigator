import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from shelltools import *
import app.analysis.assessment as assessment

from collections import defaultdict
from omorfi.omorfi import Omorfi

from polyglot.text import Text
from pytrie import StringTrie as Trie
import string

from progress import ProgressBar


class TextProcessor(object):
    # DEFAULT PROCESSOR
    def __init__(self):
        self.output_lemmas = False
        # this should remove OCR bugs but would skip "legal" punctuation also
        # might be insufficient
        self.remove = string.punctuation + '—»■„™®«§£•€□►▼♦“»★✓❖’▲°©‘*®'
        self.token_to_lemma = {}
        self.skip_item = {}
        
    def get_tokens(self, text):
        # default relies on tokenization from polyglot package
        return Text(text).words
   
    def get_lemmas(self, tokens):
        # ensures that processing is called for each string only once, to make it faster
        if self.output_lemmas:
            for t in tokens:
                if not t in self.token_to_lemma:
                    self.token_to_lemma[t] = self.get_lemma(t)
            return [self.token_to_lemma[t] for t in tokens]
        return []

    def get_lemma(token):
        # language-specific processing
        pass
        
    def skipitem (self, item):
        if not item in self.skip_item:
            # remove items that don't look like human language
            # may add more filters here in the future
            if len(item) < 2:
                self.skip_item[item] = True
            elif any(char.isdigit() for char in item):
                self.skip_item[item] = True
            elif any(char in self.remove for char in item):
                self.skip_item[item] = True
            else:
                self.skip_item[item] = False
        return self.skip_item[item]
        

class FrProcessor(TextProcessor):
    def __init__(self):
        super(FrProcessor, self).__init__()
        self.output_lemmas = True
        self.remove = self.remove.replace("'", "")  # important symbol for French
        
    def get_lemma(self, token):
        # that's not actually lemmatisation, just trying to map together words with/without articles
        # e.g. "antagonisme", "d'antagonisme", "l'antagonisme"
        return token.replace("l'", "").replace("d'", "")

    
class FinProcessor(TextProcessor):
    # simplest lemmatization for Finnish; slow and non-accurate
    # TODO: replace with processing tools that Mark is using 
    # run in parallel
    def __init__(self):
        super(FinProcessor, self).__init__()
        self.output_lemmas = True
        self.analyser_path = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                          "../venv/share/omorfi/omorfi.describe.hfst"))
        self.omorfi = Omorfi()
        self.omorfi.load_analyser(self.analyser_path)


    def get_tokens(self, text):
        # simple preprocessing (remove punctuation, replace 'w' with 'v')
        cleanup=str.maketrans('wW','vV',self.remove)
        return [t.surf.translate(cleanup) for t in self.omorfi.tokenise(text)]
    
    def get_lemma(self, token):
        # omorfi may produce more than one analyses, lets get the last one (-1)
        # lemmas are lists, can be more than one lemma (compounds) --> join
        # TODO: make proper lemma selection---we might prefer
        # compounds if the end users have special iterest in compounds
        return "".join(self.omorfi.analyse(token)[-1].lemmas)
            

LANG_PROCESSOR_MAP = defaultdict(TextProcessor,
    {'fi':FinProcessor(), 'fr':FrProcessor()}
)


class Document(object):
    def __init__(self, doc, text_processor, lang_id=None):        
        self.doc_id = doc['id']
        self.lang_id = lang_id or doc['language_ssi']
        self.text_processor = text_processor
        
        text_field = 'all_text_t' + self.lang_id + '_siv' # e.g. 'all_text_tfr_siv',
                                                          # don't know what it mean,
                                                          # let's hope it won't change
        self.text = doc[text_field]
        
        # dates are lists of strings in format 'yyyy-mm-dd'
        # why lists, could it be more than one date for a document???
        # lets take the first
        self.date = doc['date_created_ssim'][0].split('-')
        
        self.tokens = self.text_processor.get_tokens(self.text)
        # depending on language, some processors may take as an input
        # tokens, some -- raw text
        # will fix when(if) we have more text processors
        self.lemmas = self.text_processor.get_lemmas(self.tokens)

    def iter_tokens(self):
        for t in self.tokens:
            if not self.text_processor.skipitem(t):
                yield t

    def iter_lemmas(self, lowercase=True):
        for l in self.lemmas:
            if not self.text_processor.skipitem(l):
                if lowercase:
                    yield l.lower()
                else:
                    yield l
               
class Corpus(object):

    def __init__(self, lang_id, debug_count=10e100, verbose=True):
        self.lang_id = lang_id
        self.text_processor = LANG_PROCESSOR_MAP[lang_id]
        self.DEBUG_COUNT = debug_count  # limits number of documents
        self.verbose = verbose

        # stuff we want to compute only once, potentially useful for many tasks

        self.docid_to_date = {}

        self.token_to_docids = defaultdict(list)
        self.lemma_to_docids = defaultdict(list)

        # we cannot store the same document trice in memory
        # it make sense to store pre-processed documents but in the db or file system
        # we cannot load all documents in memory like that, it'll never work
        
        #self.docid_to_tokens = defaultdict(list)
        #self.docid_to_lemmas = defaultdict(list)

        # Tries are slow, so we don't use them as main storing structures
        self.prefix_token_vocabulary = Trie()
        self.prefix_lemma_vocabulary = Trie()
        self.suffix_token_vocabulary = Trie()
        self.suffix_lemma_vocabulary = Trie()

        # slow and seems to be important for analysis
        self._timeseries = {}
        

    def loop_db(self, per_page = 100, force_refresh = False):
        # 100 is a maximum number of documents per page allowed through web interface
        # currently relies on shelltools
        # TODO: integrate into main processing
        # TODO: run in parallel (in future, for really big corpora)
        page = 1

        pb = ProgressBar("Loop DB", verbose=self.verbose)
        query = {'f[language_ssi][]': self.lang_id, 'per_page':per_page}
        if force_refresh:
                query.update({'force_refresh':'T'})
        while True:
            query.update({'page':page})
            task = search(query)
            docs = task.task_result.result['docs']

            for doc in docs:
                pb.next()
                yield doc

            if task.task_result.result['pages']['last_page?']:
                break
            page += 1
        pb.end()
            

    def download_db(self):
        # dummy function for initial download
        # after running that all data will be in the local db
        for d in self.loop_db(force_refresh = True):
            pass                
                
            
    # TODO: slow, should be a separate task with results (indexes) stored in db
    # TODO: run in parallel
    def build_indexes(self):

        # build only once
        if self.docid_to_date:
            return

        doc_count = 0
        for d in self.loop_db():
            doc = Document(d, self.text_processor, self.lang_id)

            self.docid_to_date[doc.doc_id] = doc.date

            # self.docid_to_tokens[doc.doc_id] = list(doc.iter_tokens())
            for token in doc.iter_tokens():
                self.token_to_docids[token].append(doc.doc_id)

            # self.docid_to_lemmas[doc.doc_id] = list(doc.iter_lemmas())
            for lemma in doc.iter_lemmas():
                self.lemma_to_docids[lemma].append(doc.doc_id)

            doc_count += 1

            if doc_count == self.DEBUG_COUNT:

                break

    def find_word_to_doc_dict(self, item):
        if item == "token":
            return self.token_to_docids
        elif item == "lemma":
            if not self.text_processor.output_lemmas:
                raise NotImplementedError("Lemmas are not available for %s." % self.lang_id.upper())
            return self.lemma_to_docids
        else:
            raise ValueError("item must be token or lemma")

    # TIMESERIES    
    def timeseries(self, item="token", granularity="year", min_count=10, word_list=None):
        if not (item in self._timeseries and granularity in self._timeseries[item]):
            self.build_timeseries(item=item, granularity=granularity, min_count=min_count)
        elif not min_count in self._timeseries[item][granularity]:
            # let's see if there is timeseries with smaller min_count, that's would be fine as well
            smaller_min_counts = [mc for mc in self._timeseries[item][granularity].keys() \
                                     if (isinstance(mc, int) and mc < min_count)]
            if smaller_min_counts:
                # still we are storing the same information multiple times
                # TODO: rethink, how to store timeseries in a more compact way
                self._timeseries[item][granularity][min_count] = \
                                            {w:ts for w,ts in self._timeseries[item][granularity][max(smaller_min_counts)].items() \
                                             if len(self.find_word_to_doc_dict(item)[w]) >= min_count}
            else:
                self.build_timeseries(item=item, granularity=granularity, min_count=min_count)
                
                                  
        timeseries = self._timeseries[item][granularity][min_count]
        if word_list:
            word_to_docids = self.find_word_to_doc_dict(item)
            timeseries = {w:ts for w,ts in timeseries.items() if w in word_list}

        total = self._timeseries[item][granularity]['total']        

        # ipm - items per million
        # relative count (relative to all items in this date slice)
        timeseries_ipm = \
            {w: {date: (count*10e5)/total[date] for (date, count) in ts.items()} for (w, ts) in timeseries.items()}

        return timeseries, timeseries_ipm

    def build_timeseries(self, item="token", granularity="year", min_count=10):        
        gran_to_field_map = {"year": 0, "month": 1, "day": 2}
        field = gran_to_field_map[granularity]

        word_to_docids = self.find_word_to_doc_dict(item)
                
        if not self.docid_to_date:
            # TODO: build_indexes will be a separate task
            # the general controlling mechanism will take care that it has been done
            print("Indexes are not ready, building indexes...")
            self.build_indexes()

        
        # timeseries are faster to build but probably we would need to store them in self variables and reuse
        total = defaultdict(int)
        timeseries = defaultdict(lambda: defaultdict(int))

        pb = ProgressBar("Building timeseries", total = len(word_to_docids), verbose=self.verbose)
        for (w, docids) in word_to_docids.items():
            pb.next()
            for docid in docids:
                date = "-".join(self.docid_to_date[docid][:field+1])
                total[date] += 1
                if len(docids) >= min_count:
                    timeseries[w][date] += 1
        pb.end()

        if item not in self._timeseries:
            self._timeseries[item] = {}
        if granularity not in self._timeseries[item]:
            self._timeseries[item][granularity] = {}
                                        
        # need both ts and total, since total takes
        # into account everything, including items that are less
        # frequent than min_count        
        self._timeseries[item][granularity][min_count] = timeseries
        self._timeseries[item][granularity]['total'] = total

                
    # SUFFIX/PREFIX SEARCH
    @staticmethod
    def build_tries_from_dict(item_to_doc, min_count):
        prefix_trie = Trie({item: None for item in item_to_doc.keys() if len(item_to_doc[item]) >= min_count})
        suffix_trie = Trie({item[::-1]: None for item in prefix_trie.keys()})
        return prefix_trie, suffix_trie

    def build_substring_structures(self, token_min_count=10, lemma_min_count=10):
        if not self.token_to_docids:
            print("Need to build main indexes first")
            self.build_indexes()

        print("Building prefix/suffix search structures")

        # Tries are slow so we build indexes first and use frequency threshold to store only most frequent tokens
        self.prefix_token_vocabulary, self.suffix_token_vocabulary = \
            self.build_tries_from_dict(self.token_to_docids, token_min_count)
        self.prefix_lemma_vocabulary, self.suffix_lemma_vocabulary = \
            self.build_tries_from_dict(self.lemma_to_docids, lemma_min_count)

    def find_tokens_by_prefix(self, prefix):
        return self.prefix_token_vocabulary.keys(prefix=prefix)

    def find_lemmas_by_prefix(self, prefix):
        return self.prefix_lemma_vocabulary.keys(prefix=prefix)

    def find_tokens_by_suffix(self, suffix):
        # assume that user would type word in a normal left-to-right form and wants to see result in the same form
        # so we first flip the suffix, than search it in flipped dictionary than flip back the results
        return [(k[::-1]) for k in self.suffix_token_vocabulary.keys(prefix=suffix[::-1])]

    def find_lemmas_by_suffix(self, suffix):
        return [(k[::-1]) for k in self.suffix_lemma_vocabulary.keys(prefix=suffix[::-1])]


class SubCorpus(Corpus):
    # SubCorpus should behave more-or-less as Corpus but
    # should not repeat expensive operations (indexes, text
    # processing, etc.)
        
    def __init__(self, query):
        # TODO: initialize corpus with query
        # query may match documents from different languages, which would
        # require calling several textprocessors most probably we will
        # split corpus by language, i.e. will have more than one corpus
        # object for the task, since indexes are language-specific anyway
        # there is a utility for that in analysis_utils.py, SplitDocumentSetByFacet

        raise NotImplementedError
           
    
    
    
    
