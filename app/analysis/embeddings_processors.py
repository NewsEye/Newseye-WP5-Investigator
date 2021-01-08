from app.models import Processor
from app.analysis.processors import AnalysisUtility
from app.analysis import assessment
from flask import current_app
import asyncio
import requests
from config import Config
from collections import Counter
from werkzeug.exceptions import NotFound


class ExpandQuery(AnalysisUtility):
    @classmethod
    def _make_processor(cls):
        return Processor(
            name=cls.__name__,
            import_path=cls.__module__,
            description="Propose a new query by finding words most similar to keywords in the dataset",
            input_type="word_list",
            output_type="dataset_list",
            parameter_info=[
                {
                    "name": "max_number",
                    "description": "number of words in the new query. default 10",
                    "type": "integer",
                    "default": 10,
                    "required": False,
                }
            ],
        )

    async def get_input_data(self, previous_task_result):
        return {
            "langs": await self.get_languages(),
            "words": list(previous_task_result.result["vocabulary"].keys())[
                : self.task.parameters["max_number"]
            ],
        }  # not sure how many this API could handle...

    async def query_similar_words(self, query):
        uri = Config.TOPIC_MODEL_URI + "/word-embeddings/query"
        current_app.logger.debug("embeddings_request: %s" % query)
        response = requests.post(uri, json=query)
        current_app.logger.debug("response: %s" % response)
        if response.status_code == 200:
            return response.json()["similar_words"]
        else:
            return []

    async def make_result(self):

        langs = self.input_data["langs"]
        max_langs = max(langs.values())
        langs = [l for l in langs if langs[l] / max_langs > 0.25]

        queries = [
            {"lang": l, "word": word, "num_words": self.task.parameters["max_number"]}
            for word in self.input_data["words"]
            if len(word) > 3
            for l in langs
        ]

        results = await asyncio.gather(
            *[self.query_similar_words(query) for query in queries]
        )

        res = []
        for r in results:
            res.extend(r)

        if "q" in self.task.search_query:
            existed_words = self.task.search_query["q"].split()
        else:
            existed_words = []
            
        res = [r for r in res if len(r) > 3 and not r in existed_words]

        try:
            assert(res)
        except:
            raise NotFound("Nothing found for this query. Try to change input parameters")
        
        total = len(queries)
        selected = Counter(res).most_common(self.task.parameters["max_number"])
        selected = {s[0]: s[1] / total for s in selected}
        
        current_app.logger.debug("SELECTED: %s" % selected)

        if self.task.dataset:
            query = Config.SOLR_PARAMETERS["default"]
        elif self.task.search_query:
            query = self.task.search_query

            
            
        query["mm"] = 1
        query["q"] = " OR ".join(selected.keys())

        return {"query": query, "words": selected}

    async def estimate_interestingness(self):
        return self.result.pop("words")
