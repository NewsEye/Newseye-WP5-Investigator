import os, sys
from collections import defaultdict
from pytrie import StringTrie as Trie
from progress import ProgressBar

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


class Corpus(object):

    def __init__(self, lang_id, verbose=True, input_dir=None, input_format=None):
        self.lang_id = lang_id
        # self.text_processor = LANG_PROCESSOR_MAP[lang_id]
        # self.DEBUG_COUNT = debug_count  # limits number of documents
        self.verbose = verbose

        self.input_dir = input_dir
        self.input_format = input_format

        # This can be used to define only a subset of all documents for analysis
        self.target_query = {'f[language_ssi][]': self.lang_id}

        # stuff we want to compute only once, potentially useful for many tasks

        self.docid_to_date = {}

        self.token_to_docids = defaultdict(list)
        self.lemma_to_docids = defaultdict(list)

        # bigrams
        self.token_bi_to_docids = defaultdict(list)
        self.lemma_bi_to_docids = defaultdict(list)

        # Tries are slow, so we don't use them as main storing structures
        self.prefix_token_vocabulary = Trie()
        self.prefix_lemma_vocabulary = Trie()
        self.suffix_token_vocabulary = Trie()
        self.suffix_lemma_vocabulary = Trie()

        # slow and seems to be important for analysis
        self._timeseries = {}

    def set_target_query(self, query):
        self.target_query = {'f[language_ssi][]': self.lang_id}
        self.target_query.update(query)

    def find_word_to_doc_dict(self, item):
        item, *ngram = item.split('-')
        if not ngram:
            ngram = 1
        else:
            ngram = ngram[0]
        if item == "token" and ngram == '1':
            return self.token_to_docids
        elif item == "token" and ngram == '2':
            return self.token_bi_to_docids
        elif item == "lemma":
            if not self.lemma_to_docids:
                raise NotImplementedError("Lemmas are not available for %s." % self.lang_id.upper())
            elif ngram == "1":
                return self.lemma_to_docids
            elif ngram == "2":
                return self.lemma_bi_to_docids
        raise ValueError("item must be token-X or lemma-X with X=1 or 2")

    # TIMESERIES
    def timeseries(self, item="token-1", granularity="year", min_count=10, word_list=None):
        if not (item in self._timeseries and granularity in self._timeseries[item]):
            self.build_timeseries(item=item, granularity=granularity, min_count=min_count)
        elif not min_count in self._timeseries[item][granularity]:
            # let's see if there is timeseries with smaller min_count, that's would be fine as well
            smaller_min_counts = [mc for mc in self._timeseries[item][granularity].keys()
                                  if (isinstance(mc, int) and mc < min_count)]
            if smaller_min_counts:
                # still we are storing the same information multiple times
                # TODO: rethink, how to store timeseries in a more compact way
                self._timeseries[item][granularity][min_count] = \
                    {w: ts for w, ts in self._timeseries[item][granularity][max(smaller_min_counts)].items()
                     if len(self.find_word_to_doc_dict(item)[w]) >= min_count}
            else:
                self.build_timeseries(item=item, granularity=granularity, min_count=min_count)

        timeseries = self._timeseries[item][granularity][min_count]
        if word_list:
            timeseries = {w: ts for w, ts in timeseries.items() if w in word_list}

        total = self._timeseries[item][granularity]['total']

        # ipm - items per million
        # relative count (relative to all items in this date slice)
        timeseries_ipm = \
            {w: {date: (count * 10e5) / total[date] for (date, count) in ts.items()} for (w, ts) in timeseries.items()}

        return timeseries, timeseries_ipm

    def build_timeseries(self, item="token-1", granularity="year", min_count=10):
        gran_to_field_map = {"year": 0, "month": 1, "day": 2}
        field = gran_to_field_map[granularity]

        word_to_docids = self.find_word_to_doc_dict(item)

        if not self.docid_to_date:
            print("Invalid corpus: required indexes are missing")
            return

        # timeseries are faster to build but probably we would need to store them in self variables and reuse
        total = defaultdict(int)
        timeseries = defaultdict(lambda: defaultdict(int))

        pb = ProgressBar("Building timeseries", total=len(word_to_docids), verbose=self.verbose)
        for (w, docids) in word_to_docids.items():
            pb.next()
            for docid in docids:
                date = "-".join(self.docid_to_date[docid][:field + 1])
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

    # BIGRAMS

    @staticmethod
    def make_counts(w_to_docids, min_count):
        return {k: len(v) for k, v in w_to_docids.items() if len(v) >= min_count}

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

    def _find_group_by_affix(self, affix, item):
        if affix[0] == "suffix":
            if item == "lemma":
                return self.find_lemmas_by_suffix(affix[1])
            else:
                return self.find_tokens_by_suffix(affix[1])
        else:
            if item == "lemma":
                return self.find_lemmas_by_prefix(affix[1])
            else:
                return self.find_tokens_by_prefix(affix[1])

    def find_group_by_affix(self, affix, item):
        # affix is a tuple, e.g. ("suffix", "ismi"), ("prefix", "kansalais")
        group = self._find_group_by_affix(affix, item)
        try:
            assert (group)
        except:
            print("Invalid corpus: required indexes are missing")
            return None

        if not group:
            print("Group is empty, nothing found")

        return group


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
