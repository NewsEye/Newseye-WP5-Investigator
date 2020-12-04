from app.analysis.processors import AnalysisUtility
from app.models import Processor
from app.utils.search_utils import search_database
from app.analysis import assessment
from flask import current_app
from collections import defaultdict
from math import sqrt
import asyncio


class ExtractNames(AnalysisUtility):
    @classmethod
    def _make_processor(cls):
        return Processor(
            name=cls.__name__,
            import_path=cls.__module__,
            description="Extracts most salient names from a given collection",
            input_type="dataset",
            output_type="name_list",
            parameter_info=[
                {
                    "name": "max_number",
                    "description": "number of names to return. 0 means all. default 10",
                    "type": "integer",
                    "default": 10,
                    "required": False,
                },
                {
                    "name": "sort_by",
                    "description": "how to sort entities, default 'salience'",
                    "type": "string",
                    "default": "salience",
                    "required": False,
                    "values": ["salience", "stance"],
                },
            ],
        )

    @staticmethod
    async def get_name(entity):
        query = {
            "fl": "label_fi_ssi,label_fr_ssi,label_sv_ssi,label_de_ssi",
            "fq": "id:%s" % entity,
        }
        res = await search_database(query)
        return (
            entity,
            {
                k.replace("label_", "").replace("_ssi", ""): v
                for k, v in res["docs"][0].items()
            },
        )

    async def get_input_data(self):
        if self.task.dataset:
            docids = [d.document.solr_id for d in self.task.dataset.documents]
        elif self.task.search_query:
            search = await search_database(self.task.search_query, retrieve="docids")
            docids = [d["id"] for d in search["docs"]]

        query = {
            "q": "*:*",
            "fq": "{!terms f=article_id_ssi}" + ",".join([d_id for d_id in docids]),
        }

        return await search_database(query, retrieve="names")

    async def make_result(self):
        # current_app.logger.debug("INPUT_DATA: %s" %self.input_data)

        doc_mentions = defaultdict(list)

        for mention in self.input_data["docs"]:
            doc_mentions[mention["article_id_ssi"]].append(
                {
                    "ent": mention["linked_entity_ssi"]
                    if mention["linked_entity_ssi"]
                    else mention["mention_ssi"],
                    "stance": mention["stance_fsi"],
                    "start_position": mention["article_index_start_isi"],
                }
            )

        max_positions = {
            d: max([m["start_position"] for m in doc_mentions[d]]) for d in doc_mentions
        }

        saliences = defaultdict(list)
        stances = defaultdict(list)

        for doc, mentions in doc_mentions.items():
            # compute salience within document:
            starts = defaultdict(list)
            for em in mentions:
                stances[em["ent"]].append(em["stance"])
                starts[em["ent"]].append(em["start_position"])
            for e, ss in starts.items():

                prominence = 1 - min(ss) / (max_positions[doc] + 10.0)
                frequency = len(ss) / len(doc_mentions[doc])
                saliences[e].append(sqrt(prominence * frequency))

        # average:
        # stances are averaged by mentions
        stances = {e: sum(ss) / len(ss) for e, ss in stances.items()}

        # salience is averaged by total number of documents
        saliences = {e: sum(ss) / len(doc_mentions) for e, ss in saliences.items()}

        result = {}
        count = 0
        max_number = self.task.parameters.get("max_number")

        get_name_calls = []

        if self.task.parameters.get("sort_by") == "salience":
            sort_key = saliences.get
        elif self.task.parameters.get("sort_by") == "stance":
            sort_key = lambda e: (abs(stances[e]), saliences[e])

        for e in sorted(saliences, key=sort_key, reverse=True):
            result[e] = {"salience": saliences[e], "stance": stances[e]}

            if e.startswith("entity_"):
                get_name_calls.append(self.get_name(e))

            count += 1
            if max_number and count == max_number:
                break

        names = await asyncio.gather(*get_name_calls)
        for ent in names:
            result[ent[0]]["names"] = ent[1]

        return result

    async def estimate_interestingness(self):
        return {
            ent: {"salience": res["salience"], "stance": abs(res["salience"])}
            for ent, res in self.result.items()
        }
