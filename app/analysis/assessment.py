import numpy as np
import sys

from collections import Iterable

EPSILON = sys.float_info.epsilon  # smallest possible number

class Distribution(object):   
    def __init__(self, data, smoothing = None):
        self.smoothing = smoothing
        self.dist = self.make_distribution(data)
        self.entropy = -np.sum((self.dist*np.log2(self.dist)))
        self.number_of_outcomes = len(self.dist)
        
    def make_distribution(self, list_of_counts):
        # Normalize a non-negative discrete distribution onto the
        # range [0-1], ensuring no zero value.
        arr = np.array(list_of_counts)
        
        # TODO: smoothing factor depending on number of outcomes
        smoothing_factor = self.smoothing if self.smoothing else EPSILON
        return (arr + smoothing_factor) / (np.sum(arr) + len(arr) * smoothing_factor)

    @property
    def normalized_entropy(self):
        if self.number_of_outcomes == 1:
            return 0
        return self.entropy/np.log2(self.number_of_outcomes)

    
def ensure_distributions(*dist):
    ret = []
    for d in dist:
        if isinstance(d, Distribution):
            ret.append(d)
        else:
            ret.append(Distribution(d))
    return ret
            
            
# DISTRIBUTION COMPARISON METRICS
    
def kl_divergence(p,q):
    p,q = ensure_distributions(p,q)
    return np.sum(p.dist*np.log2(p.dist/q.dist))

def kl_distance(p,q):
    p,q = ensure_distributions(p,q)
    # this one is symmetrical
    return np.sum((p.dist-q.dist)*np.log2(p.dist/q.dist))
    
def normalized_kl_divergence(p,q):
    p,q = ensure_distributions(p,q)
    return kl_divergence(p.dist,q.dist) / (np.log2(q.number_of_outcomes)-q.entropy())
                
def cross_entropy(p,q):
    p,q = ensure_distributions(p,q)
    return -np.sum((p.dist*np.log2(q.dist)))


# DICTIONARY COMPARISON METHODS

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
    return {k:float(dict1[k])/dict2[k] for k in dict1.keys()}
    
def weighted_frequency_ratio(dict1, dict2, weights=None, weight_func=np.log10): 
    # TODO: slowish, check what's going on
    # maybe switch to np arrays, pandas, whatever
    # frequency ratio where more weight given to some cases
    # default weight is a log10 of denominator, the bigger denominator (e.g. corpus frequency) the mpre relyable considered result
    # maybe better to replace all hacks with statistical significance
    if not weights:
        weights = dict2
    fr = frequency_ratio(dict1, dict2)
    # wfr = (fr - 1) * weight + 1
    # fr = 1 is a neutral value, thus (fr - 1) for that cases would be zero
    # and not magnified by weighting
    return {k:((fr[k]-1)*weight_func(weights[k])+1) for k in dict1.keys()}  

def find_large_numbers(data, coefficient=2):
    # dummy function, most probably will be replaced with something more clever
    # at least we can use this one as a baseline
    vals = list(data.values())
    mean = np.mean(vals)
    std = np.std(vals)
    return {k:v for (k,v) in data.items() if (v - mean) > coefficient*std}

def find_large_numbers_from_lists(lists, coefficient=2):
    # works differently than the previous function, returns big numbers mask
    # used for topic modelling, might be useful for smth else
    arr = np.array(lists)
    mean, std = np.mean(arr), np.std(arr)
    mask = np.zeros(arr.shape)
    mask[np.where(arr - mean > coefficient*std)] = 1
    return mask.tolist()
    
