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

    print ("Needs further investigations... stay tuned!")
    print ("******************************************************\n")           
