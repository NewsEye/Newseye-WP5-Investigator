import os, sys
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from textprocessing.textprocessing import Corpus
import app.analysis.assessment as assessment
import app.analysis.timeseries as timeseries


##### EXAMPLES #######
import numpy as np

def load_corpora():
    # SLOOOOW
    # run this function in advance, before showing actual fun with other functions
    fr = Corpus('fr')
    de = Corpus('de')
    fi = Corpus('fi')

    for corp in [fr]:   #[fr, de, fi]:
        corp.build_substring_structures()

    return fr, de, fi


def stay_tuned():
    print ("Needs further investigations... stay tuned!")
    print ("******************************************************\n")           



def ism(corpus, word = 'patriotisme', suffix = 'isme'):  
    # TODO: make impressive example, add plots    
    print ("\n******************************************************")
    print ("Corpus: %s, word: '%s', group: all words with suffix '%s'" \
           %(corpus.lang_id, word, suffix))
    
    group = corpus.find_lemmas_by_suffix(suffix)
    counts = {w:len(corpus.lemma_to_docids[w]) for w in group}
    print("Words with suffix '%s', sorted by count:" %suffix)
    for (w,c) in sorted(counts.items(), key=lambda x: x[1], reverse = True):
        print (w, c)

    wfr = timeseries.compare_word_to_group(corpus, word, group)

    ts, ts_ipm = corpus.timeseries('lemma', 'month')

    
    group_ts = timeseries.sum_up({w:ts[w] for w in group})
    group_ts_ipm = timeseries.sum_up({w:ts_ipm[w] for w in group})

    print ("'%s': averaged count %3.2f, averaged relative count (ipm) %3.2f" \
         %(word, np.mean(list(ts[word].values())), np.mean(list(ts_ipm[word].values()))))
    print ("'%s': averaged count %3.2f, averaged relative count (ipm) %3.2f" \
         %(suffix, np.mean(list(group_ts.values())), np.mean(list(group_ts_ipm.values()))))

    spikes = assessment.find_large_numbers(wfr)
    print("Potentially interesting dates:")
    for k in sorted(spikes, key = lambda k: wfr[k], reverse = True):
        print("%s: '%s': %d (%2.2f ipm), '%s': %d (%2.2f ipm)"\
          %(k, word, ts[word][k], ts_ipm[word][k], suffix, group_ts[k], group_ts_ipm[k]))

    stay_tuned()

def group_outliers(corpus, suffix="isme", weights=False):
    # try running this function with and without weights 
    # 'gargarisme' is the act of bubbling liquid in the mouth
    # for more details see: https://fr.wikipedia.org/wiki/Gargarisme

    print ("\n******************************************************")
    print ("Corpus: %s, group: all words with suffix '%s'" %(corpus.lang_id, suffix))
    group = corpus.find_lemmas_by_suffix(suffix)
    print("Words with suffix '%s': " %suffix, group)

    if weights:
        weights = {w:np.log10(len(corpus.lemma_to_docids[w])) for w in group}
    
    outliers = timeseries.find_group_outliers(corpus, group, weights = weights)
    print("Group outliers: ")
    
    ts, _ = corpus.timeseries("lemma", "month", word_list = outliers.keys())
    for w in outliers:
        print("")
        print (w, ts[w])


    stay_tuned()
        

def interesting_words(corpus):
    # TODO:
    # 1. select words with count more than smth
    # 2. find the most interesting words
    # 3. find the most interestind dates for these words
    pass
    
