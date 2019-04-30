import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from shelltools import *

from collections import defaultdict
import json
from omorfi.omorfi import Omorfi
import string

from polyglot.text import Text

class TextProcessor(object):
    # DEFAULT PROCESSOR
    def __init__(self):
        self.output_lemmas = False
        # this should remove OCR bugs but would skip "legal" punctuation also
        # might be insufficient
        self.remove = string.punctuation + '—»■„™®«§£•€□►▼♦“»★✓❖’▲°©‘*®'
        
    def get_tokens(self, text):
        # default relies on tokenization from polyglot package
        return Text(text).words

    def get_lemmas(self, token):
        # depending on language, some processors may take as an input tokens, some -- raw text
        # TODO: fix when(if) we have more text processors
        return []

    def skipitem (self, item):
    # remove items that don't look like human language
    # may add more filters here in the future
        if len(item) < 2:
            return True
        if any(char.isdigit() for char in item):
            return True
        if any(char in self.remove for char in item):
            return True
        return False
        

class FrProcessor(TextProcessor):
    def __init__(self):
        super(FrProcessor, self).__init__()
        self.remove = self.remove.replace("'", "")  # import symbol for French

    
class FinProcessor(TextProcessor):
    # simplest lemmatization for Finnish
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
    
    def get_lemmas(self, tokens):
        # omorfi may produce more than one analyses, lets get the last one (-1)
        # lemmas are lists, can be more than one lemma (compounds) --> join
        # TODO: make proper lemma selection---we might prefer
        # compounds if the end users have special iterest in compounds
        return ["".join(self.omorfi.analyse(t)[-1].lemmas) for t in tokens]
            

LANG_PROCESSOR_MAP = defaultdict(lambda: TextProcessor(),
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
    def __init__(self, lang_id, debug_count=10e100):
        self.lang_id = lang_id
        self.text_processor = LANG_PROCESSOR_MAP[lang_id]
        self.DEBUG_COUNT = debug_count

        # stuff we want to compute only once, potentially useful for many tasks
        self.docid_to_date = {}
        self.token_to_docids = defaultdict(list)
        self.lemma_to_docids = defaultdict(list)
        
                
    def loop_db(self, per_page = 100):
        # currently relies on shelltools
        # TODO: integrate into main processing
        # TODO: run in parallel (in future, for really big corpora)
        page = 1
    
        while(True):
            print("page: ", page)
            
            task = search({'f[language_ssi][]': self.lang_id, 'page':page, 'per_page':per_page})
            docs = task.task_result.result['response']['docs']
            
            for doc in docs:
                yield doc
                
            if task.task_result.result['response']['pages']['last_page?']:
                break
            page += 1
                
    def download_db(self):
            # dummy function for initial download
            # after running that all data will be in the local db
            for d in self.loop_db():
                pass

        
    # TODO: slow, should be a separate task with results (indexes) stored in db
    # TODO: run in parallel
    def build_indexes(self):

        # build only once
        if self.docid_to_date:
            return
        
        count = 0
        for d in self.loop_db():
            doc = Document(d, self.text_processor, self.lang_id)
            print(doc.doc_id)
            
            self.docid_to_date[doc.doc_id] = doc.date
            
            for t in doc.iter_tokens():
                self.token_to_docids[t].append(doc.doc_id)

            for l in doc.iter_lemmas():
                self.lemma_to_docids[l].append(doc.doc_id)

            count += 1
            if count==self.DEBUG_COUNT:
                break
            


    def build_time_series(self, item="token", granularity = "year", min_count = 10):
        gran_to_field_map = {"year" : 0, "month" : 1, "day" : 2}
        field = gran_to_field_map[granularity]
         
        if item == "token":
            word_to_docids = self.token_to_docids
        elif item == "lemma":
            if not self.text_processor.output_lemmas:
                raise NotImplementedError("Lemmas are not available for %s." %self.lang_id.upper())
            word_to_docids = self.lemma_to_docids
        else:
            raise ValueError("item must be token or lemma")

        if not self.docid_to_date:
            # TODO: build_indexes will be a separate task, the general
            # controlling mechanism will take care that it has been done
            print("Indexes are not ready, building indexes...")
            self.build_indexes()


        total = defaultdict(lambda: 0)
        timeseries = defaultdict(lambda: defaultdict(lambda: 0))
        for (w, docids) in word_to_docids.items():           
            if len(docids) <= min_count:
                record = False
            else:
                record = True

            for docid in docids:
                # record only words that are frequent enough
                # but count everything for total counts
                date = "-".join(self.docid_to_date[docid][:field+1])
                total[date] += 1
                if record:
                    timeseries[w][date] += 1

        # ipm - items per million
        # relative count (relative to all items in this date slice)
        timeseries_ipm = {w: {date: (count*10e5)/total[date] for (date, count) in ts.items()} for (w, ts) in timeseries.items()}
        
        # TODO: write to db    
        return json.loads(json.dumps(timeseries)), json.loads(json.dumps(timeseries_ipm)), dict(total)
            
