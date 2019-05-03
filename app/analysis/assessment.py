import numpy as np
import sys

class Comparison(object):
    def __init__(self):
        pass
                
    @staticmethod
    def align_dicts_from_to(from_dict, to_dict, default_value=0.0):
        # insert missed values, so that all keys from_dict have values in to_dict
        for k in from_dict.keys():
            if k not in to_dict.keys():
                to_dict[k] = default_value 
    
    @staticmethod
    def align_dicts(dict1, dict2, default_value=0.0):
        Comparison.align_dicts_from_to(dict1, dict2, default_value)
        Comparison.align_dicts_from_to(dict2, dict1, default_value)


class Distribution(object):
    EPSILON = sys.float_info.epsilon  # smallest possible number

    def __init__(self, data):
        self.dist = self.normalize_distribution(data)        

    def normalize_distribution(self, list_of_counts):
        """Normalize a discrete distribution onto the range [0-1], ensuring no zero 
        values.
        """
        dist = np.array(list_of_counts)               
        dist = (l + EPSILON) / (np.sum(l) + len(l) * EPSILON)
        assert(np.sum(dist)==1)
        return dist

    
        
        
        
