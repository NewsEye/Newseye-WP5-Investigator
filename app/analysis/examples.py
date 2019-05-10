from textprocessing.textprocessing import Corpus
from app.analysis import assessment, timeseries

import numpy as np
import pickle

##### EXAMPLES #######


def build_corpus(language, query=None):
    # SLOOOOW
    # run this function in advance, before showing actual fun with other functions
    corpus = Corpus(language)
    if query:
        corpus.set_target_query(query)
    corpus.build_substring_structures()

    return corpus


def store_corpus_to_pickle(corpus, filename):
    """
    Store the dicts in the specified corpus into pickle files for faster retrieval until we get the database set up.
    :param corpus: the corpus to be stored
    :param filename: the descriptive part for the pickled files, etc. "fi_uusisuometar". Needs to start with the language code.
    :return: 0 if everything went fine.
    """
    picklepath = 'pickled/'
    with open('{}_docid_to_date.pickle'.format(picklepath + filename), 'wb') as f:
        pickle.dump(corpus.docid_to_date, f, protocol=pickle.HIGHEST_PROTOCOL)
    with open('{}_lemma_to_docids.pickle'.format(picklepath + filename), 'wb') as f:
        pickle.dump(corpus.lemma_to_docids, f, protocol=pickle.HIGHEST_PROTOCOL)
    with open('{}_token_to_docids.pickle'.format(picklepath + filename), 'wb') as f:
        pickle.dump(corpus.token_to_docids, f, protocol=pickle.HIGHEST_PROTOCOL)
    with open('{}_prefix_lemma_vocabulary.pickle'.format(picklepath + filename), 'wb') as f:
        pickle.dump(corpus.prefix_lemma_vocabulary, f, protocol=pickle.HIGHEST_PROTOCOL)
    with open('{}_suffix_lemma_vocabulary.pickle'.format(picklepath + filename), 'wb') as f:
        pickle.dump(corpus.suffix_lemma_vocabulary, f, protocol=pickle.HIGHEST_PROTOCOL)
    with open('{}_prefix_token_vocabulary.pickle'.format(picklepath + filename), 'wb') as f:
        pickle.dump(corpus.prefix_token_vocabulary, f, protocol=pickle.HIGHEST_PROTOCOL)
    with open('{}_suffix_token_vocabulary.pickle'.format(picklepath + filename), 'wb') as f:
        pickle.dump(corpus.suffix_token_vocabulary, f, protocol=pickle.HIGHEST_PROTOCOL)
    return corpus


def load_corpus_from_pickle(filename):
    corpus = Corpus(filename[:2])
    picklepath = 'pickled/'
    with open('{}_docid_to_date.pickle'.format(picklepath + filename), 'rb') as f:
        corpus.docid_to_date = pickle.load(f)
    with open('{}_lemma_to_docids.pickle'.format(picklepath + filename), 'rb') as f:
        corpus.lemma_to_docids = pickle.load(f)
    with open('{}_token_to_docids.pickle'.format(picklepath + filename), 'rb') as f:
        corpus.token_to_docids = pickle.load(f)
    with open('{}_prefix_lemma_vocabulary.pickle'.format(picklepath + filename), 'rb') as f:
        corpus.prefix_lemma_vocabulary = pickle.load(f)
    with open('{}_suffix_lemma_vocabulary.pickle'.format(picklepath + filename), 'rb') as f:
        corpus.suffix_lemma_vocabulary = pickle.load(f)
    with open('{}_prefix_token_vocabulary.pickle'.format(picklepath + filename), 'rb') as f:
        corpus.prefix_token_vocabulary = pickle.load(f)
    with open('{}_suffix_token_vocabulary.pickle'.format(picklepath + filename), 'rb') as f:
        corpus.suffix_token_vocabulary = pickle.load(f)
    return corpus


def stay_tuned():
    print ("Needs further investigations... stay tuned!")
    print ("******************************************************\n")           


def print_group_count(corpus, item, group):
    if item == 'lemma':
        counts = {w:len(corpus.lemma_to_docids[w]) for w in group}
    else:
        counts = {w:len(corpus.token_to_docids[w]) for w in group}
        
    print("Group words, sorted by count:")
    for (w,c) in sorted(counts.items(), key=lambda x: x[1], reverse = True):
        print (w, c)

    
def ism(corpus, word = 'patriotisme', affix=("suffix", "isme"),
                      item="lemma", granularity="month"):  
    # TODO: make impressive example, add plots    
    print ("\n******************************************************")
    print ("Corpus: %s, word: '%s', group: all words with %s '%s'" \
           %(corpus.lang_id, word, affix[0], affix[1]))
    
    group = corpus.find_group_by_affix(affix, item)
    if not group: return
    
    print_group_count(corpus, item, group)    
    
    wfr = timeseries.compare_word_to_group(corpus, word, group, item=item)

    
    ts, ts_ipm = corpus.timeseries(item, granularity)

    
    group_ts = timeseries.sum_up({w:ts[w] for w in group})
    group_ts_ipm = timeseries.sum_up({w:ts_ipm[w] for w in group})

    print ("'%s': averaged count %3.2f, averaged relative count (ipm) %3.2f" \
         %(word, np.mean(list(ts[word].values())), np.mean(list(ts_ipm[word].values()))))
    print ("'%s': averaged count %3.2f, averaged relative count (ipm) %3.2f" \
         %(affix[1], np.mean(list(group_ts.values())), np.mean(list(group_ts_ipm.values()))))

    spikes = assessment.find_large_numbers(wfr)
    print("Potentially interesting dates:")
    for k in sorted(spikes, key = spikes.get, reverse = True):
        print("%s: '%s': %d (%2.2f ipm), '%s': %d (%2.2f ipm)"\
          %(k, word, ts[word][k], ts_ipm[word][k], affix[1], group_ts[k], group_ts_ipm[k]))

    stay_tuned()

def group_outliers(corpus,
                   item="lemma",
                   granularity="month",
                   affix=("suffix", "isme"),
                   weights=False):
    # try running this function with and without weights 
    # 'gargarisme' is the act of bubbling liquid in the mouth
    # for more details see: https://fr.wikipedia.org/wiki/Gargarisme

    #TODO: add smoothing
    
    print ("\n******************************************************")
    print ("Corpus: %s, group: all words with '%s' '%s'" %(corpus.lang_id, affix[0], affix[1]))

    group = corpus.find_group_by_affix(affix, item)
    if not group: return
    
    print_group_count(corpus, item, group)

    
    if weights:
        word_to_docid = corpus.find_word_to_doc_dict(item)
        weights = {w:np.log10(len(word_to_docid[w])) for w in group}
    
    outliers = timeseries.find_group_outliers(corpus, group,
                                              weights = weights, item=item,
                                              granularity = granularity)
    print("Group outliers: ")
    
    ts, _ = corpus.timeseries(item, granularity, word_list = outliers.keys())
    for w in outliers:
        print("")
        print (w, dict(ts[w]))


    stay_tuned()

    
def find_interesting_words(corpus, item="lemma", granularity="month", min_count = 10,
                           threshold = 0.7, coefficient=1.2, smoothing=1):

    # when smoothing is used (highly recommended) min_count is needed
    # only to speed up comutations
    normalized_entropy = timeseries.normalized_entropy_for_aligned_ts_ipm(
        corpus=corpus,
        item=item,
        granularity=granularity,
        min_count=min_count,
        smoothing=smoothing)

    ts, ts_ipm = corpus.timeseries(item, granularity, min_count=min_count)
    total = corpus._timeseries[item][granularity]['total']

    word_to_docid = corpus.find_word_to_doc_dict(item)
    
    print ("\n******************************************************")
    print ("The most interesting words in corpus '%s'" %corpus.lang_id)
    
    # the smaller normalized_entropy, the more interesting word is
    # small NE means that probability mass is concentrated on some particular dates
    for (w, ne) in sorted(normalized_entropy.items(),
                          key = lambda x: (x[1], -len(word_to_docid[x[0]]))):
        if 1 - ne < threshold:
            break
        print ("")
        print (w)
        print ('interestness %2.2f' %(1-ne)) # entropy 0 means word is only used in a certain date; superinteresting
        print("Potentially interesting dates:")

        assessment.align_dicts_from_to(total, ts_ipm[w])
        assessment.align_dicts_from_to(total, ts[w])
        
        interesting_dates = assessment.find_large_numbers(ts_ipm[w], coefficient=coefficient)
        for date in sorted(interesting_dates):
            print("%s: %d (%2.2f ipm)" %(date, ts[w][date], ts_ipm[w][date]))

        print("average count in other dates: %2.2f (%2.2fipm)"
              %(np.mean([ts[w][d]     for d in ts[w]     if d not in interesting_dates]),
                np.mean([ts_ipm[w][d]   for d in ts_ipm[w] if d not in interesting_dates])))
        print("total count: %d" %len(word_to_docid[w]))
        
    stay_tuned()
