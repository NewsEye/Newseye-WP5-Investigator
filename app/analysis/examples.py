import os

from textprocessing.textprocessing import Corpus
from app.analysis import assessment, timeseries

import numpy as np
import pickle
import pandas as pd
import matplotlib.pyplot as plt
from math import isnan, sqrt
from collections import defaultdict
from progress import ProgressBar


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
    with open('{}_token_bi_to_docids.pickle'.format(picklepath + filename), 'wb') as f:
        pickle.dump(corpus.token_bi_to_docids, f, protocol=pickle.HIGHEST_PROTOCOL)
    with open('{}_lemma_bi_to_docids.pickle'.format(picklepath + filename), 'wb') as f:
        pickle.dump(corpus.lemma_bi_to_docids, f, protocol=pickle.HIGHEST_PROTOCOL)
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
    try:
        with open('{}_token_bi_to_docids.pickle'.format(picklepath + filename), 'rb') as f:
            corpus.token_bi_to_docids = pickle.load(f)
        with open('{}_lemma_bi_to_docids.pickle'.format(picklepath + filename), 'rb') as f:
            corpus.lemma_bi_to_docids = pickle.load(f)
    except FileNotFoundError:
        pass

    return corpus


def mz_fwt(x, n=3):
    """
    A modified version of the code at https://github.com/thomasbkahn/step-detect:

    Computes the multiscale product of the Mallat-Zhong discrete forward
    wavelet transform up to and including scale n for the input data x.
    If n is even, the spikes in the signal will be positive. If n is odd
    the spikes will match the polarity of the step (positive for steps
    up, negative for steps down).
    This function is essentially a direct translation of the MATLAB code
    provided by Sadler and Swami in section A.4 of the following:
    http://www.dtic.mil/dtic/tr/fulltext/u2/a351960.pdf
    Parameters
    ----------
    x : numpy array
        1 dimensional array that represents time series of data points
    n : int
        Highest scale to multiply to
    Returns
    -------
    prod : numpy array
        The multiscale product for x
    """
    n_pnts = x.size
    lambda_j = [1.5, 1.12, 1.03, 1.01][0:n]
    if n > 4:
        lambda_j += [1.0] * (n - 4)

    h = np.array([0.125, 0.375, 0.375, 0.125])
    g = np.array([2.0, -2.0])

    gn = [2]
    hn = [3]
    for j in range(1, n):
        q = 2 ** (j - 1)
        gn.append(q + 1)
        hn.append(3 * q + 1)

    s = x
    prod = np.ones(n_pnts)
    prods = np.ones((n, n_pnts))
    for j in range(n):
        s = np.concatenate((s[::-1], s, s[::-1]))
        n_zeros = 2 ** j - 1
        gz = insert_zeros(g, n_zeros)
        hz = insert_zeros(h, n_zeros)
        current = (1.0 / lambda_j[j]) * np.convolve(s, gz)
        current = current[n_pnts + gn[j] - 1:2 * n_pnts + gn[j] - 1]
        prod *= current
        prods[j] *= current
        s_new = np.convolve(s, hz)
        s = s_new[n_pnts + hn[j] - 1:2 * n_pnts + hn[j] - 1]
    prod /= np.abs(prod).max()
    return prod, prods


def insert_zeros(x, n):
    """
    From https://github.com/thomasbkahn/step-detect.
    Helper function for mz_fwt. Splits input array and adds n zeros
    between values.
    """
    newlen = (n + 1) * x.size
    out = np.zeros(newlen)
    indices = list(range(0, newlen - n, n + 1))
    out[indices] = x
    return out


def find_steps(array, threshold=None):
    """
    A modified version of the code at https://github.com/thomasbkahn/step-detect.

    Finds local maxima by segmenting array based on positions at which
    the threshold value is crossed. Note that this thresholding is
    applied after the absolute value of the array is taken. Thus,
    the distinction between upward and downward steps is lost. However,
    get_step_sizes can be used to determine directionality after the
    fact.
    Parameters
    ----------
    array : numpy array
        1 dimensional array that represents time series of data points
    threshold : int / float
        Threshold value that defines a step. If no threshold value is specified, it is set to 2 standard deviations
    Returns
    -------
    steps : list
        List of indices of the detected steps
    """
    if threshold is None:
        threshold = 4 * np.var(array)  # Use 2 standard deviations as the threshold
    steps = []
    above_points = np.where(array > threshold, 1, 0)
    below_points = np.where(array < -threshold, 1, 0)
    ap_dif = np.diff(above_points)
    bp_dif = np.diff(below_points)
    pos_cross_ups = np.where(ap_dif == 1)[0]
    pos_cross_dns = np.where(ap_dif == -1)[0]
    neg_cross_dns = np.where(bp_dif == 1)[0]
    neg_cross_ups = np.where(bp_dif == -1)[0]
    # If cross_dns is longer that cross_ups, the first entry in cross_dns is zero, which will cause a crash
    if len(pos_cross_dns) > len(pos_cross_ups):
        pos_cross_dns = pos_cross_dns[1:]
    if len(neg_cross_ups) > len(neg_cross_dns):
        neg_cross_ups = neg_cross_ups[1:]
    for upi, dni in zip(pos_cross_ups, pos_cross_dns):
        steps.append(np.argmax(array[upi: dni]) + upi + 1)
    for dni, upi in zip(neg_cross_dns, neg_cross_ups):
        steps.append(np.argmin(array[dni: upi]) + dni + 1)
    return sorted(steps)


# TODO: instead of using just the original data, perhaps by odd-symmetric periodical extension??
#  This should improve the accuracy close to the beginning and end of the signal
def get_step_sizes(array, indices, window=1000):
    """
    A modified version of the code at https://github.com/thomasbkahn/step-detect.

    Calculates step size for each index within the supplied list. Step
    size is determined by averaging over a range of points (specified
    by the window parameter) before and after the index of step
    occurrence. The directionality of the step is reflected by the sign
    of the step size (i.e. a positive value indicates an upward step,
    and a negative value indicates a downward step). The combined
    standard deviation of both measurements (as a measure of uncertainty
    in step calculation) is also provided.
    Parameters
    ----------
    array : numpy array
        1 dimensional array that represents time series of data points
    indices : list
        List of indices of the detected steps (as provided by
        find_steps, for example)
    window : int, optional
        Number of points to average over to determine baseline levels
        before and after step.
    Returns
    -------
    step_sizes : list
        List of tuples describing the mean of the data before and after the detected step
    step_error : list
    """
    step_sizes = []
    step_error = []
    indices = sorted(indices)
    last = len(indices) - 1
    for i, index in enumerate(indices):
        if len(indices) == 1:
            q = min(window, index, len(array) - 1 - index)
        elif i == 0:
            q = min(window, indices[i + 1] - index, index)
        elif i == last:
            q = min(window, index - indices[i - 1], len(array) - 1 - index)
        else:
            q = min(window, index - indices[i - 1], indices[i + 1] - index)
        a = array[index - q: index + 1]
        b = array[index: index + q + 1]
        step_sizes.append((a.mean(), b.mean()))
        error = sqrt(a.var() + b.var())
        if isnan(error):
            step_error.append(abs(step_sizes[-1][1] - step_sizes[-1][0]))
        else:
            step_error.append(error)
    return step_sizes, step_error


def step_analysis(keyword, corpus=None, df=None):
    if corpus:
        ts, ts_ipm = corpus.timeseries(word_list=[keyword])
        df = pd.DataFrame(ts_ipm)
        df.index = pd.to_numeric(df.index)
        index_start = df.index.min()
        index_end = df.index.max()
        idx = np.arange(index_start, index_end + 1)
        df = df.reindex(idx, fill_value=0)
    if df is None:
        return
    idx = df.index
    prod, prods = mz_fwt(df[keyword])
    steps = find_steps(prod)
    step_years = idx[steps]
    step_sizes = get_step_sizes(df[keyword], steps)
    return prod, steps, step_sizes


def plot_wavelets(keyword, corpus=None, df=None):
    if corpus:
        ts, ts_ipm = corpus.timeseries(word_list=[keyword])
        df = pd.DataFrame(ts_ipm)
        df.index = pd.to_numeric(df.index)
        index_start = df.index.min()
        index_end = df.index.max()
        idx = np.arange(index_start, index_end + 1)
        df = df.reindex(idx, fill_value=0)
    if df is None:
        return
    idx = df.index
    prod, prods = mz_fwt(df[keyword])
    f, (ax1, ax2, ax3, ax4, ax5) = plt.subplots(5, 1, sharex=True)
    ax1.set_title('Wavelets for {}'.format(keyword))
    ax1.plot(idx, df[keyword])
    ax2.plot(idx, prods[0])
    ax3.plot(idx, prods[1])
    ax4.plot(idx, prods[2])
    ax5.plot(idx, prod)
    f.show()


def plot_estimate(keyword, corpus=None, df=None):
    prod, steps, step_sizes = step_analysis(keyword, corpus=corpus, df=df)
    if corpus:
        ts, ts_ipm = corpus.timeseries(word_list=[keyword])
        df = pd.DataFrame(ts_ipm)
        df.index = pd.to_numeric(df.index)
        index_start = df.index.min()
        index_end = df.index.max()
        idx = np.arange(index_start, index_end + 1)
        df = df.reindex(idx, fill_value=0)
    if df is None:
        return
    idx = df.index
    threshold = 4 * np.var(prod)
    estimate = np.zeros(len(idx))
    for i, s in enumerate(steps):
        sizes = step_sizes[0][i]
        estimate[s:] += sizes[1] - sizes[0]
    f, (ax1, ax2, ax3) = plt.subplots(3, 1, sharex=True)
    ax1.plot(idx, df[keyword])
    ax1.set_title('Word frequencies and estimate of the steps for {}'.format(keyword))
    ax2.plot(idx, prod)
    ax2.hlines([threshold, -threshold], idx[0], idx[-1])
    ax3.plot(idx, estimate)
    f.show()
    return estimate


def research_lemma_steps(corpus, verbose=True, num_lemmas=None):
    pb = ProgressBar("Research lemmas", verbose=verbose)
    lemmas = [(key, len(docids)) for key, docids in corpus.lemma_to_docids.items()]
    lemmas.sort(key=lambda x: x[1], reverse=True)
    if num_lemmas:
        lemmas = lemmas[:num_lemmas]
    pb.total = len(lemmas)
    steps_by_year = defaultdict(list)
    for i, lemma in enumerate(lemmas):
        pb.next()
        try:
            _, steps, step_sizes = step_analysis(corpus, lemma[0])
        except ValueError:
            continue
        steps_by_year[tuple(steps)].append(lemma[0])
    pb.end()
    return steps_by_year


def stay_tuned():
    print("Needs further investigations... stay tuned!")
    print("******************************************************\n")


def print_group_count(corpus, item, group):
    if item == 'lemma':
        counts = {w: len(corpus.lemma_to_docids[w]) for w in group}
        total = len(corpus.lemma_to_docids)
    else:
        counts = {w: len(corpus.token_to_docids[w]) for w in group}
        total = len(corpus.token_to_docids)

    print("Group words, sorted by count:")
    for (w, c) in sorted(counts.items(), key=lambda x: x[1], reverse=True):
        print(w, c, "%2.2fipm" % (c * 1000000 / total))


def ism(corpus, word='patriotisme', affix=("suffix", "isme"),
        item="lemma", granularity="month"):
    # TODO: make impressive example, add plots    
    print("\n******************************************************")
    print("Corpus: %s, word: '%s', group: all words with %s '%s'" \
          % (corpus.lang_id, word, affix[0], affix[1]))

    group = corpus.find_group_by_affix(affix, item)
    if not group: return

    print_group_count(corpus, item, group)

    wfr = timeseries.compare_word_to_group(corpus, word, group, item=item)

    ts, ts_ipm = corpus.timeseries(item, granularity)

    group_ts = timeseries.sum_up({w: ts[w] for w in group})
    group_ts_ipm = timeseries.sum_up({w: ts_ipm[w] for w in group})

    print("'%s': averaged count %3.2f, averaged relative count (ipm) %3.2f" \
          % (word, np.mean(list(ts[word].values())), np.mean(list(ts_ipm[word].values()))))
    print("'%s': averaged count %3.2f, averaged relative count (ipm) %3.2f" \
          % (affix[1], np.mean(list(group_ts.values())), np.mean(list(group_ts_ipm.values()))))

    spikes = assessment.find_large_numbers(wfr)
    print("Potentially interesting dates:")
    for k in sorted(spikes, key=spikes.get, reverse=True):
        print("%s: '%s': %d (%2.2f ipm), '%s': %d (%2.2f ipm)" \
              % (k, word, ts[word][k], ts_ipm[word][k], affix[1], group_ts[k], group_ts_ipm[k]))

    stay_tuned()


def group_outliers(corpus,
                   item="lemma",
                   granularity="month",
                   affix=("suffix", "isme"),
                   weights=False):
    # try running this function with and without weights 
    # 'gargarisme' is the act of bubbling liquid in the mouth
    # for more details see: https://fr.wikipedia.org/wiki/Gargarisme

    # TODO: add smoothing

    print("\n******************************************************")
    print("Corpus: %s, group: all words with '%s' '%s'" % (corpus.lang_id, affix[0], affix[1]))

    group = corpus.find_group_by_affix(affix, item)
    if not group: return

    print_group_count(corpus, item, group)

    if weights:
        word_to_docid = corpus.find_word_to_doc_dict(item)
        weights = {w: np.log10(len(word_to_docid[w])) for w in group}

    outliers = timeseries.find_group_outliers(corpus, group,
                                              weights=weights, item=item,
                                              granularity=granularity)
    print("Group outliers: ")

    ts, _ = corpus.timeseries(item, granularity, word_list=outliers.keys())
    for w in outliers:
        print("")
        print(w, dict(ts[w]))

    stay_tuned()


def find_interesting_words(corpus, item="lemma", granularity="month", min_count=10,
                           threshold=0.5, coefficient=1.2, smoothing=0.5):
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

    print("\n******************************************************")
    print("The most interesting words in corpus '%s'" % corpus.lang_id)

    # the smaller normalized_entropy, the more interesting word is
    # small NE means that probability mass is concentrated on some particular dates
    for (w, ne) in sorted(normalized_entropy.items(),
                          key=lambda x: (x[1], -len(word_to_docid[x[0]]))):
        if 1 - ne < threshold:
            break
        print("")
        print(w)
        print('interestness %2.2f' % (1 - ne))  # entropy 0 means word is only used in a certain date; superinteresting
        print("Potentially interesting dates:")

        assessment.align_dicts_from_to(total, ts_ipm[w])
        assessment.align_dicts_from_to(total, ts[w])

        interesting_dates = assessment.find_large_numbers(ts_ipm[w], coefficient=coefficient)
        for date in sorted(interesting_dates):
            print("%s: %d (%2.2f ipm)" % (date, ts[w][date], ts_ipm[w][date]))

        print("average count in other dates: %2.2f (%2.2fipm)"
              % (np.mean([ts[w][d] for d in ts[w] if d not in interesting_dates]),
                 np.mean([ts_ipm[w][d] for d in ts_ipm[w] if d not in interesting_dates])))
        print("total count: %d" % len(word_to_docid[w]))

    stay_tuned()
    # 


def print_top_counts(counts_dict, min_count=0, top=1000):
    iterator = 1
    for k in sorted(counts_dict, key=counts_dict.get, reverse=True):
        if iterator == top or counts_dict[k] < min_count:
            break
        print("%s, %0.2f" % (k, counts_dict[k]))
        iterator += 1


def print_top(dict_of_mentions, min_count=1, top=1000):
    print_top_counts({k: len(v) for k, v in dict_of_mentions.items()
                      if len(v) >= min_count}, min_count)


def dump_corpus(corpus, corpus_name=None, output_dir="dump/"):
    #### dump corpus (for Elaine, for TM, maybe we will use it for smth else)

    if not corpus_name: corpus_name = corpus.lang_id
    output_dir = os.path.join(os.path.abspath(output_dir), corpus.lang_id)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print("Dumping corpus into %s" % output_dir)

    for doc in corpus.readin_docs(process=False):
        with open(os.path.join(output_dir, doc.doc_id), 'w') as out:
            print(doc.doc_id, file=out)
            print("-".join(doc.date[:3]), file=out)
            print(doc.text, file=out)
