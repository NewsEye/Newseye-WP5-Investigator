import pandas as pd
import numpy as np

from app.analysis import assessment
from textprocessing.textprocessing import Corpus

def tf_idf(subcorpus, reference, item="token", default_df = 0.1, normalization=True, alpha = 0.4):
    # assumed that reference corpus is much bigger and general in
    # terms of topics and word distributions
    # subcorpus in principle can be taken from smth else than
    # reference but it is assumed that it is much smaller
   
    # if both corpora are subcorpora it may be better to compare using
    # wfr or divergence


    # TODO: bigrams
    if item == "token":
        tf  = subcorpus.token_tf()
        df = reference.token_df()
    elif item == "lemma":
        tf  = subcorpus.lemma_tf()
        df = reference.lemma_df()

    N = len(reference.docid_to_date) # total number of documents in the reference corpus

    # in case word is not found in the reference it's df set to 0.1
    # there could be other ways to weight/smooth idf
    frame = pd.DataFrame.from_dict(
        [{"w":w, "tf":tf[w], "df":df[w] if w in df else default_df} for w in tf],
        dtype = float)

    frame.idf = np.log(N/frame.df)

    if normalization:
        # maximum tf normalization as described here:
        # https://nlp.stanford.edu/IR-book/pdf/06vect.pdf
        #could be other methods
        #doesn't look very useful when comparing one subcorpus to a
        #reference, but might be crusial if there is more than one
        #subcorpus
        frame.ntf = alpha + (1-alpha)*(frame.tf/frame.tf.max())              
        frame["tfidf"] = frame.ntf*frame.idf
    else:
        frame["tfidf"] = frame.tf*frame.idf
                        

    return dict(zip(frame.w, frame.tfidf))

def _ipm(counts):
    tot = np.sum([float(v) for v in counts.values()])
    return {k:v*1000000/tot for (k,v) in counts.items()}

def fr_comparison(corpus1, corpus2, item="token", weights = None, zero_value = 0.1):
    # doesn't seem to produce meaningful results, needs more thinking
    
    if item == "lemma":
        counts1 = Corpus.make_counts(corpus1.lemma_to_docids, 0)
        counts2 = Corpus.make_counts(corpus2.lemma_to_docids, 0)
    elif item == "token":
        counts1 = Corpus.make_counts(corpus1.token_to_docids, 0)
        counts2 = Corpus.make_counts(corpus2.token_to_docids, 0)
    elif item == "lemma2":
        counts1 = Corpus.make_counts(corpus1.lemma_bi_to_docids, 0)
        counts2 = Corpus.make_counts(corpus2.lemma_bi_to_docids, 0)
    elif item == "token2":
        counts1 = Corpus.make_counts(corpus1.token_bi_to_docids, 0)
        counts2 = Corpus.make_counts(corpus2.token_bi_to_docids, 0)


        
    assessment.align_dicts(counts1, counts2, zero_value)

    ipm1 = _ipm(counts1)
    ipm2 = _ipm(counts2)
        
    if not weights:
        return assessment.frequency_ratio(ipm1, ipm2)
        
    if weights == 1:
        weights = ipm1
    elif weights == 2:
        weights = ipm2
    else:
        weights = weights

    return assessment.weighted_frequency_ratio(ipm1, ipm2, weights, weight_func = lambda x: x)
    
    

    
