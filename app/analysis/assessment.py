import numpy as np
import sys
from flask import current_app
from collections import Iterable

EPSILON = sys.float_info.epsilon  # smallest possible number


class Distribution(object):
    def __init__(self, data, smoothing=None):
        self.smoothing = smoothing if smoothing else EPSILON
        self.dist = self.make_distribution(data)
        self.entropy = -np.sum((self.dist * np.log2(self.dist)))
        self.number_of_outcomes = len(self.dist)

    def make_distribution(self, numbers):
        # Normalize a non-negative discrete distribution onto the
        # range [0-1], ensuring no zero value.
        arr = np.array(list(numbers))
        minimum = np.amin(arr)
        if minimum < 0:
            # shift to zero:
            arr = arr - minimum

        # TODO: smoothing factor depending on number of outcomes
        smoothing_factor = self.smoothing
        return (arr + smoothing_factor) / (np.sum(arr) + len(arr) * smoothing_factor)

    @property
    def normalized_entropy(self):
        if self.number_of_outcomes == 1:
            return 0
        return self.entropy / np.log2(self.number_of_outcomes)


def ensure_distribution(d):
    if isinstance(d, Distribution):
        return d
    return Distribution(d)


def ensure_distributions(*dist):
    return [ensure_distribution(d) for d in dist]


# DISTRIBUTION COMPARISON METRICS
# TODO: Wasserstain distance


def normalized_entropy(p):
    p = ensure_distribution(p)
    return p.normalized_entropy


def kl_divergence(p, q):
    p, q = ensure_distributions(p, q)
    return np.sum(p.dist * np.log2(p.dist / q.dist))


def kl_distance(p, q):
    p, q = ensure_distributions(p, q)
    # this one is symmetrical
    return np.sum((p.dist - q.dist) * np.log2(p.dist / q.dist))


def js_divergence(p, q):
    p, q = ensure_distributions(p, q)
    M = ensure_distributions([(p.dist[i] + q.dist[i]) / 2 for i in range(len(p.dist))])[
        0
    ]
    return kl_divergence(p, M) / 2 + kl_divergence(q, M) / 2


def normalized_kl_divergence(p, q):
    p, q = ensure_distributions(p, q)
    return kl_divergence(p.dist, q.dist) / (np.log2(q.number_of_outcomes) - q.entropy())


def cross_entropy(p, q):
    p, q = ensure_distributions(p, q)
    return -np.sum((p.dist * np.log2(q.dist)))


# DICTIONARY COMPARISON METHODS


def dicts_to_comparable_dist(dict1, dict2):
    # assume dicts are aligned
    p = [dict1[k] for k in dict1]
    q = [dict2[k] for k in dict1]  # ensure order
    return p, q


def dict_normalized_kl_divergence(dict1, dict2):
    p, q = dicts_to_comparable_dist(dict1, dict2)
    return normalized_kl_divergence(p, q)


def dict_kl_distance(dict1, dict2):
    p, q = dicts_to_comparable_dist(dict1, dict2)
    return kl_distance(p, q)


def dict_js_divergence(dict1, dict2):
    p, q = dicts_to_comparable_dist(dict1, dict2)
    return js_divergence(p, q)


def align_dicts_from_to(from_dict, to_dict, default_value=0.0):
    # insert missed values, so that all keys from_dict have values in to_dict
    for k in from_dict.keys():
        if k not in to_dict.keys():
            to_dict[k] = default_value


def align_dicts(dict1, dict2, default_value=0.0):
    align_dicts_from_to(dict1, dict2, default_value)
    align_dicts_from_to(dict2, dict1, default_value)


def frequency_ratio(dict1, dict2):
    # align_dicts(dict1, dict2, EPSILON)  # seems that alignment is always done outside this function with some special precaution
    return {k: float(dict1[k]) / dict2[k] for k in dict1.keys()}


def weighted_frequency_ratio(dict1, dict2, weights=None, weight_func=np.log10):
    # TODO: slowish, check what's going on
    # maybe switch to np arrays, pandas, whatever
    # frequency ratio where more weight given to some cases
    # default weight is a log10 of denominator, the bigger denominator (e.g. corpus frequency) the more reliable considered result
    # maybe better to replace all hacks with statistical significance
    if not weights:
        weights = dict2
    fr = frequency_ratio(dict1, dict2)
    # wfr = (fr - 1) * weight + 1
    # fr = 1 is a neutral value, thus (fr - 1) for that cases would be zero
    # and not magnified by weighting
    return {k: ((fr[k] - 1) * weight_func(weights[k]) + 1) for k in dict1.keys()}


def find_large_numbers(data, coefficient=2):
    # dummy function, most probably will be replaced with something more clever
    # at least we can use this one as a baseline
    vals = list(data.values())
    mean = np.mean(vals)
    std = np.std(vals)
    return {k: v for (k, v) in data.items() if (v - mean) > coefficient * std}


def find_large_numbers_from_lists(lists, coefficient=2):
    # works differently than the previous function, returns big numbers mask
    arr = np.array(lists)
    mean, std = np.mean(arr), np.std(arr)
    mask = np.zeros(arr.shape)
    mask[np.where(arr - mean > coefficient * std)] = 1
    return mask.tolist()


# INTERESTINGNESS
def recoursive_max(data):
    if not data:
        return 0.0
    if isinstance(data, str):
        return 0.0
    elif isinstance(data, float):
        return data
    elif type(data) in [list, tuple, set]:
        return max([recoursive_max(i) for i in data])
    elif isinstance(data, dict):
        return recoursive_max([recoursive_max(i) for i in data.values()])
    else:
        return data


def max_interestingness(interestingness):
    if not interestingness:
        return 0.0
    return recoursive_max(interestingness)


def recoursive_distribution(data):
    """
    Loop through data, converts numerical lists into distributions
    """

    if not data:
        return 0.0
    if isinstance(data, str):
        return 0.0
    if type(data) in [float, int]:
        return 0 if data == 0 else 1
    if isinstance(data, dict):
        if all([type(i) in [float, int] for i in data.values()]):
            return {
                k: v
                for (k, v) in zip(
                    data.keys(), recoursive_distribution(list(data.values()))
                )
            }
        return {k: recoursive_distribution(v) for k, v in data.items()}
    if type(data) in [list, tuple, set]:
        if all([type(i) in [float, int] for i in data]):
            return Distribution(data).dist
        return [recoursive_distribution(i) for i in data]
