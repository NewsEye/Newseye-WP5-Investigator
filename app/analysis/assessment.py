import numpy as np
import sys

from collections import Iterable

class Distribution(object):
    EPSILON = sys.float_info.epsilon  # smallest possible number
    
    def __init__(self, data):
        self.dist = self.make_distribution(data)
        self.entropy = -np.sum((self.dist*np.log2(self.dist)))
        self.number_of_outcomes = len(self.dist)
                               
    def make_distribution(self, list_of_counts):
        # Normalize a discrete distribution onto the range [0-1], ensuring no zero values.
        arr = np.array(list_of_counts)               
        return (arr + EPSILON) / (np.sum(arr) + len(arr) * EPSILON)
    
def ensure_distributions(*dist):
    ret = []
    for d in dist:
        if isinstance(data, Distribution):
            ret.append(data)
        else:
            ret.append(Distribution(data))

            
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

def frequency_ratio(dict1, reference):
    align_dicts_from_to(reference, dict1)
    return {k:dict1[k]/v for k,v in reference}
    
def weighted_frequency_ratio(dict1, reference, weights=None):
    align_dicts_from_to(reference, dict1)
    if not weights: weights = reference
    return {k:dict1[k]*log(weights[k])/v for k,v in reference}
