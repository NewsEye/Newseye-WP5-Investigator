from app.models import Processor
from app.analysis.processors import AnalysisUtility
from app.utils.search_utils import search_database
from app.analysis import assessment
from math import log, exp
from collections import defaultdict
from flask import current_app
import asyncio

class WordProcessor(AnalysisUtility):
    @classmethod
    def _make_processor(cls):
        return Processor(
            name=cls.__name__,
            import_path=cls.__module__,
            parameter_info=[
                {
                    "name": "unit",
                    "description": "which unit --- token or stem --- should be used for analysis",
                    "type": "string",
                    "default": "stems",
                    "required": False,
                }
            ],
            input_type="dataset",
        )

    async def get_input_data(self, solr_query):
            return await search_database(solr_query, retrieve=self.task.parameters["unit"])



class ExtractWords(WordProcessor):
    @classmethod
    def _make_processor(cls):
        processor = super()._make_processor()
        description="Finds all the different words in the input document set, their counts and weights.",
        processor.output_type = "word_list"
        return processor
    
    async def make_result(self):
        """
        Builds word dictionary for the dataset
        Takes as an input document-wise dictionaries and compiles them into a single dictionary for the dataset.
        """
        # TODO: might need to save an initial dictionary for reuse

        df = {}
        tf = defaultdict(int)
        total = 0.0
        # Note: df that came from SOLR are computing using the whole
        # (multilingual) collection. Might need to do it language-wise
        # (slower?)
        for word_dict in list(self.input_data.values()):
            for word, info in word_dict.items():
                df[word] = info["df"]
                tf[word] += info["tf"]
                total += info["tf"]
                         # abs      rel                     tf-idf
        result = {word: (tf[word], tf[word]/total, tf[word] * log(total / df[word])) for word in tf}
        return {
            "total": int(total),
            "vocabulary": {  # sort by tf-idf:
                k: result[k] for k in sorted(result, key=lambda x: (result[x][2], x), reverse=True)
            },
        }

    async def estimate_interestingness(self):
        vocab = self.result["vocabulary"]
        return assessment.recoursive_distribution(
            {word: exp(vocab[word][2]) for word in vocab}  
        )  # interestingness based on tf-idf, the biggest numbers highlighted



class ExtractBigrams(WordProcessor):
    # could be more efficient
    # could use frequency thresholds for bigrams based on word counts computed in the ExtractWord processors
    # lets make it working first and them decide if optimization is needed
    @classmethod
    def _make_processor(cls):
        processor = super()._make_processor()
        description="Finds all the different bigrams in the input document set, their counts and weights.",            
        processor.output_type = "bigram_list"
        return processor

    async def make_result(self):
        word_count = defaultdict(int)
        bigram_count = defaultdict(int)
        total = 0.0
        for doc_processing in asyncio.as_completed([self.collect_document_counts(doc_dict)
                                                    for doc_dict in self.input_data.values()]):
            doc_word_count, doc_bigram_count = await doc_processing

            for word in doc_word_count:
                word_count[word] += doc_word_count[word]
                total += doc_word_count[word]
            for bigram in doc_bigram_count:
                bigram_count[bigram] += doc_bigram_count[bigram]

                
        dice_score = {
            b : 2.0 * bigram_count[b] / (word_count[b[0]] + word_count[b[1]])
            for b in bigram_count
        }

        return { " ".join(b) : (bigram_count[b], bigram_count[b]/total, dice_score[b]) # sort by dice_score:
                  for b in sorted(dice_score, key=lambda b: dice_score[b], reverse=True)}

    @staticmethod
    async def collect_document_counts(doc_dict):
        position_to_word = {}
        for word, info in doc_dict.items():
            for pos in info['positions']:
                position_to_word[pos] = word
        word_list = [position_to_word[p] for p in sorted(position_to_word)]
        word_count = defaultdict(int)
        bigram_count = defaultdict(int)
        
        for i in range(len(word_list)-1):
            word_count[word_list[i]] += 1
            bigram_count[(word_list[i], word_list[i+1])] +=1
        word_count[word_count[-1]] += 1 

        return word_count, bigram_count

    async def estimate_interestingness(self):
        return assessment.recoursive_distribution(
            #   dice_score * log(count)
            {b: self.result[b][1]*log(self.result[b][0]) for b in self.result}
            )


