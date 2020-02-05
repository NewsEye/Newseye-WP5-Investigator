
class Bigrams(object):
    @staticmethod
    def mention_dict_to_count(dict_of_mentions, min_count=0):
        return {k: len(v) for k, v in dict_of_mentions.items() if len(v) >= min_count}

    def __init__(self, word_dict, bigram_dict, min_count=3):
        self.word_dict = self.mention_dict_to_count(word_dict)
        self.bigram_dict = self.mention_dict_to_count(bigram_dict, min_count)

        # measures of collocation strength
        self.dice = {}
        # TODO: linguistic mutual information, claimed to be the most suitable in the literature
        self.LMI = {}
        self.MI = {}
        self.Tscore = {}
        self.LL = {}
        # there might be more

    def compute_dice(self):
        self.dice = {
            b: 2 * self.bigram_dict[b] / (self.word_dict[b[0]] + self.word_dict[b[1]])
            for b in self.bigram_dict
        }

    def dice(self, bigram):
        if not self.dice:
            self.compute_dice()
        return self.dice[bigram]

    def print_measure(self, measure_dict, reverse=True, min_value=0):
        # for now, for debugging, later on we might want to return a json object instead
        for m in sorted(measure_dict, key=measure_dict.get, reverse=reverse):
            if measure_dict[m] < min_value:
                break
            print("%s %s\t%0.2f\t%0.2f" % (m[0], m[1], measure_dict[m], self.bigram_dict[m]))

    def print_dice(self, min_value=0.5):
        self.print_measure(self.dice, min_value=min_value)
