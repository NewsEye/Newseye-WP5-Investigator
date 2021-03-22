from app.analysis.processors import AnalysisUtility
from app.models import Processor
from app.analysis import assessment
from flask import current_app
from collections import defaultdict
import numpy as np
import asyncio
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import io
import base64

STANCE_TYPES = ["NEG", "NEU", "POS"]
COLORS = ["r", "grey", "b"]


class NameProcessor(AnalysisUtility):
    async def query_mentions_for_collection(self):
        if self.task.dataset:
            docids = [d.document.solr_id for d in self.task.dataset.documents]
        elif self.task.search_query:
            search = await self.search_database(
                self.task.search_query, retrieve="docids"
            )
            docids = [d["id"] for d in search["docs"]]

        query = {
            "q": "*:*",
            "fq": "{!terms f=article_id_ssi}" + ",".join([d_id for d_id in docids]),
        }

        return await self.search_database(query, retrieve="names")

    async def get_name(self, entity):
        query = {
            "fl": "label_fi_ssi,label_fr_ssi,label_sv_ssi,label_de_ssi,label_en_ssi",
            "fq": "id:%s" % entity,
        }
        res = await self.search_database(query, retrieve="name_info")

        #current_app.logger.debug("RES: %s" %res)
        
        if res:
            entity_info = {k.replace("label_", "").replace("_ssi", ""): v for k, v in res[0].items()}
        else:
            entity_info = {}
            
        return (
            entity,
            entity_info
        )


class ExtractNames(NameProcessor):
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

    async def get_input_data(self):
        return await self.query_mentions_for_collection()

    async def make_result(self):
        #current_app.logger.debug("INPUT_DATA: %s" %self.input_data)

        doc_mentions = defaultdict(list)

        for mention in self.input_data:
            #current_app.logger.debug("MENTION: %s" %mention)
            doc_mentions[mention["article_id_ssi"]].append(
                {
                    "ent": mention["linked_entity_ssi"] or mention["mention_ssi"],
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
                saliences[e].append(np.sqrt(prominence * frequency))

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

        names = await asyncio.gather(*get_name_calls, return_exceptions=(not current_app.debug))
        for ent in names:
            result[ent[0]]["names"] = ent[1]

        return result

    async def estimate_interestingness(self):
        return {
            ent: {"salience": res["salience"], "stance": abs(res["salience"])} if res else 0.0
            for ent, res in self.result.items()
        }


class TrackNameSentiment(NameProcessor):
    @classmethod
    def _make_processor(cls):
        return Processor(
            name=cls.__name__,
            import_path=cls.__module__,
            description="Builds sentiment timeseries for most salient names in the collection",
            parameter_info=[],
            input_type="name_list",
            output_type="timeseries",
        )

    async def get_input_data(self, previous_task_result):

        # current_app.logger.debug("PREVIOUS_TASK_RESULT.RESULT %s" %previous_task_result.result)

        # non-optimal, this query has been done already...
        mentions = await self.query_mentions_for_collection()

        if not mentions:
            return

        mentions = [
            m
            for m in mentions
            if m["linked_entity_ssi"] in previous_task_result.result
            or m["mention_ssi"] in previous_task_result.result
        ]
        docids = set([m["article_id_ssi"] for m in mentions])

        # current_app.logger.debug("MENTIONS: %s" %len(mentions))
        # current_app.logger.debug("DOCIDS: %s" %len(docids))

        # query year for each doc --- is it possible to avoid somehow???
        query = {
            "q": "*:*",
            "fq": "{!terms f=id}" + ",".join([docid for docid in docids]),
            "fl": "year_isi, id",
        }

        res = await self.search_database(query, retrieve="docids")
        doc_to_year = {r["id"]: int(r["year_isi"]) for r in res["docs"]}
        years = list(doc_to_year.values())

        min_y, max_y = min(years), max(years)

        # for each entity: create an array 3 sentiments times years
        mention_data = defaultdict(lambda: np.zeros((max_y - min_y + 1, 3)))

        year_index = {y: i for i, y in enumerate(range(min_y, max_y + 1))}
        stance_index = {-1.0: 0, 0.0: 1, 1.0: 2}

        nonneutral_count = defaultdict(int)

        for mention in mentions:
            name_id = mention["linked_entity_ssi"] or mention["mention_ssi"]
            year = doc_to_year[mention["article_id_ssi"]]
            stance = mention["stance_fsi"]
            mention_data[name_id][year_index[year], stance_index[stance]] += 1
            if stance != 0.0:
                nonneutral_count[name_id] += 1

        selected_mentions = {
            m: mention_data[m]
            for m in sorted(
                mention_data, key=lambda m: nonneutral_count[m], reverse=True
            )
            if nonneutral_count[m] > 0
        }

        return {
            "start_year": min_y,
            "end_year": max_y,
            "entity_timeseries": selected_mentions,
            "entity_info": {
                e: i
                for e, i in previous_task_result.result.items()
                if e in selected_mentions
            },
        }

    def visualize_stance_media_evol(self, entity, plot_type):
        # current_app.logger.debug("ENTITY: %s PLOT_TYPE: %s" %(entity, plot_type))
        s = io.BytesIO()

        start_year = self.input_data["start_year"]
        end_year = self.input_data["end_year"]

        length = end_year - start_year + 1

        fig, ax = plt.subplots(1, 1)

        locs, labels = plt.xticks()
        year_offset = (
            1 if int(length / locs.shape[0]) == 0 else int(length / locs.shape[0])
        )
        new_list_years = list(
            range(start_year, end_year + year_offset + 1, year_offset)
        )

        xx = np.array([i * year_offset for i in list(range(len(new_list_years)))])
        plt.xticks(xx, [str(i) for i in new_list_years])

        stance_arr = self.input_data["entity_timeseries"][entity]

        ax.set_title(
            "Stance evolution of "
            + entity
            + " from "
            + str(start_year)
            + " to "
            + str(end_year)
        )
        if plot_type == "line":
            max_of_max_arr = np.max(np.max(stance_arr, axis=1))
            max_arr = np.repeat(max_of_max_arr, stance_arr.shape[0])
            visual_data = (stance_arr[:, 0] - stance_arr[:, 2]) / max_arr
            ax.plot(range(0, length), visual_data)
            ax.set_ylim([-1, 1])

        elif plot_type == "bar":
            width = 0.25
            for counter, item in enumerate(stance_arr):
                for i in range(3):
                    ax.bar(counter + width * i, item[i], width, color=COLORS[i])

            ax.legend(STANCE_TYPES, loc="upper right")

        ax.set_ylabel("Stance polarity")
        ax.set_xlabel("Year")
        ax.axhline(0, color="black", lw=0.5)

        plt.savefig(s, format="png", bbox_inches="tight")
        plt.close()
        s = base64.b64encode(s.getvalue()).decode("utf-8").replace("\n", "")

        return s

    async def make_images(self):
        images = {}

        for entity in self.input_data["entity_timeseries"]:
            images[entity] = {
                "line": self.visualize_stance_media_evol(entity, "line"),
                "bar": self.visualize_stance_media_evol(entity, "bar"),
            }

        return images

    async def make_result(self):
        start_y = self.input_data["start_year"]
        end_y = self.input_data["end_year"]

        ent_sentiment = defaultdict(dict)
        for ent, ts in self.input_data["entity_timeseries"].items():
            for i, y in enumerate(range(start_y, end_y + 1)):
                sentiment = ts[i][2] - ts[i][0]
                ent_sentiment[ent][y] = sentiment / sum(ts[i]) if sentiment else 0.0

            try:
                ent_sentiment[ent]["names"] = self.input_data["entity_info"][ent].get(
                    "names"
                )
            except KeyError as err:
                current_app.logger.debug(
                    "Unknown entity in TrackNameSentiment: %s" % ent
                )
                name = await self.get_name(ent)
                ent_sentiment[ent]["names"] = name[1]
        return dict(ent_sentiment)

    async def estimate_interestingness(self):
        interestingness = defaultdict(dict)
        start_y = self.input_data["start_year"]
        end_y = self.input_data["end_year"]
        for ent, ts in self.input_data["entity_timeseries"].items():
            # tot = np.sum(ts)
            # MORE WEIGHT TO NON-NEUTRAL:
            tot = sum(ts[:, 0]) * 10 + sum(ts[:, 1]) + sum(ts[:, 2]) * 10

            for i, y in enumerate(range(start_y, end_y + 1)):
                # interestingness[ent][y] = np.sum(ts[i]) / tot
                interestingness[ent][y] = (
                    ts[i, 0] * 10 + ts[i, 1] + ts[i, 2] * 10
                ) / tot

        return {
            k: (
                1 - assessment.normalized_entropy(v.values()),
                v,
            )  # we want minimal entropy, which means sharpest peak
            for k, v in interestingness.items()
        }
