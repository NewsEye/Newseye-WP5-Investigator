from app.models import Processor
from app.analysis.processors import AnalysisUtility
from app.utils.search_utils import search_database
from app.analysis import assessment
from math import log, exp
from collections import defaultdict


class ExtractWords(AnalysisUtility):
    @classmethod
    def _make_processor(cls):
        return Processor(
            name=cls.__name__,
            import_path=cls.__module__,
            description="Finds all the different words in the input document set, their counts and weights.",
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
            output_type="word_list",
        )

    async def get_input_data(self, solr_query):
            return await search_database(solr_query, retrieve=self.task.parameters["unit"])
        

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
        result = {word: (tf[word], tf[word] * log(total / df[word])) for word in tf}
        return {
            "total": int(total),
            "vocabulary": {  # sort by tf-idf:
                k: result[k] for k in sorted(result, key=lambda x: (result[x][1], x), reverse=True)
            },
        }

    async def estimate_interestingness(self):
        vocab = self.result["vocabulary"]
        return assessment.recoursive_distribution(
            {word: exp(vocab[word][1]) for word in vocab}
        )  # interestingness based on tf-idf



class ExtractBigrams(AnalysisUtility):
    @classmethod
    def _make_processor(cls):
        return Processor(
            name=cls.__name__,
            import_path=cls.__module__,
            description="Finds all the different bigrams in the input document set, their counts and weights.",
            parameter_info=[
                {
                    "name": "unit",
                    "description": "which unit --- token or stem --- should be used for analysis",
                    "type": "string",
                    "default": "stem",
                    "required": False,
                }
            ],
            input_type="dataset",
            output_type="bigram_list",  ## is it the same type as word list?
        )


    
    
