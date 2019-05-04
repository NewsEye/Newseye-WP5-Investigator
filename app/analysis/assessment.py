import numpy as np
import sys

from collections import Iterable

EPSILON = sys.float_info.epsilon  # smallest possible number

class Distribution(object):   
    def __init__(self, data):
        self.dist = self.make_distribution(data)
        self.entropy = -np.sum((self.dist*np.log2(self.dist)))
        self.number_of_outcomes = len(self.dist)
                               
    def make_distribution(self, list_of_counts):
        # Normalize a non-negative discrete distribution onto the
        # range [0-1], ensuring no zero value.
        arr = np.array(list_of_counts)               
        return (arr + EPSILON) / (np.sum(arr) + len(arr) * EPSILON)

def timeseries_entropy(ts):
    # convert timeseries to distribution
    dist = Distribution([ts[k] for k in sorted(ts)])
    return dist.entropy()
    
    
def ensure_distributions(*dist):
    ret = []
    for d in dist:
        if isinstance(data, Distribution):
            ret.append(data)
        else:
            ret.append(Distribution(data))
    return ret
            
            
# DISTRIBUTION COMPARISON METRICS
    
def kl_divergence(p,q):
    p,q = ensure_distributions(p,q)
    return np.sum(p*np.log2(p/q))

def kl_distance(p,q):
    p,q = ensure_distributions(p,q)
    # this one is symmetrical
    return np.sum((p-q)*np.log2(p/q))
    
def normalized_kl_divergence(p,q):
    p,q = ensure_distributions(p,q)
    return kl_divergence(p,q) / (np.log2(q.number_of_outcomes)-q.entropy())
                
def cross_entropy(p,q):
    p,q = ensure_distributions(p,q)
    return -np.sum((p*np.log2(q)))


# DICTIONARY ALIGNMENT AND COMPARISON METHODS
    
def align_dicts_from_to(from_dict, to_dict, default_value=0.0):
    # insert missed values, so that all keys from_dict have values in to_dict
    for k in from_dict.keys():
        if k not in to_dict.keys():
            to_dict[k] = default_value 
                            
def align_dicts(dict1, dict2, default_value=0.0):
    align_dicts_from_to(dict1, dict2, default_value)
    align_dicts_from_to(dict2, dict1, default_value)

def frequency_ratio(dict1, dict2):
    align_dicts(dict1, dict2, EPSILON)
    return {k:float(dict1[k])/dict2[k] for k in dict1.keys()}
    
def weighted_frequency_ratio(dict1, dict2, weights=None, weight_func=np.log10):
    # frequency ratio where more weight given to some cases
    # default weight is a log10 of denominator, the bigger denominator (e.g. corpus frequency) the mpre relyable considered result
    # maybe better to replace all hacks with statistical significance
    if not weights:
        weights = dict2
    fr = frequency_ratio(dict1, dict2)
    return {k:fr[k]*weight_func(weights[k]) for k in dict1.keys()}  

def find_spikes(ts):
    # dummy function, most probably will be replaced with something more clever
    # at least we can use this one as a baseline
    vals = [ts[k] for k in sorted(ts)]
    mean = np.mean(vals)
    std = np.std(vals)
    return [k for k in ts if np.mean(ts[k] - mean) > 2*std]
    
    
