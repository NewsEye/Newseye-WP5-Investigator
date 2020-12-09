from app.analysis.processors import AnalysisUtility
from app.models import Processor
from flask import current_app
from collections import defaultdict

from app.analysis.summarization.textrank import *
from app.analysis.summarization.mmr import *
import app.analysis.summarization.data_util as data_util

import spacy


class Summarization(AnalysisUtility):
    @classmethod
    def _make_processor(cls):
        return Processor(
            name=cls.__name__,
            import_path=cls.__module__,
            description="Summarizes set of texts. Not recommended to feed more than 25 documents",
            parameter_info=[
                {
                    "name": "summary_length",
                    "type": "integer",
                    "default": 40,
                    "description": "Summary length",
                    "required": False,
                },
                {
                    "name": "similarity_threshold",
                    "type": "float",
                    "default": 0.5,
                    "description": "Threshold to consider two sentences as similar",
                    "required": False,
                },
                {
                    "name": "type_summary",
                    "type": "string",
                    "default": "ai",
                    "values": ["ai", "mr"],
                    "description": "Most relevant (mr) or additional information (ai)",
                    "required": False,
                },
                {
                    "name": "minimal_sentence_length",
                    "type": "integer",
                    "default": 10,
                    "description": "Sentences shorter than this value won't be analyzed",
                    "required": False,
                },
                {
                    "name": "type_sentence_representation",
                    "type": "string",
                    "default": "mean",
                    "values": ["mean", "sum"],
                    "description": "Type of sentence representation using word embeddings: 'sum' (Sum all word embeddings in a sentence) or 'mean' (Mean of all word embeddings in a sentence). Default is 'mean'",
                    "required": False,
                },
                {
                    "name": "ts_approach",
                    "type": "string",
                    "default": "textrank",
                    "values": ["textrank", "mmr"],
                    "description": "Text Summarization approach: 'textrank' or 'mmr' (Maximal Marginal Relevance).",
                    "required": False,
                },
            ],
            input_type="dataset",
            output_type="text",
        )

    async def make_result(self):
        """
        Makes a summary of article texts
        """

        texts = defaultdict(list)
        for article in self.input_data["docs"]:
            lang = article["language_ssi"]
            text = article["all_text_t" + lang + "_siv"]
            texts[lang].append(text)

        # summarization done only for documents in the same language
        lang = sorted(texts, key=lambda x: len(texts[x]), reverse=True)[0]

        # current_app.logger.debug("LANG: %s" %lang)

        texts = texts[lang]

        # current_app.logger.debug("TEXTS: %s" %texts)

        language = lang if lang in ["en", "fr"] else "xx_ent_wiki_sm"

        # current_app.logger.debug("LANGUAGE: %s" %language)

        nlp = spacy.load(language)

        # -------- Preprocessing texts -------- #
        document = data_util.process_document(
            document=texts, nlp=nlp, language=language
        )
        document = [" ".join([w.text for w in nlp(s)]) for d in document for s in d]
        document = [
            s
            for s in document
            if len(s.split()) >= self.task.parameters["minimal_sentence_length"]
        ]  # remove short sentences

        # -------- Clean sentences -------- #
        clean_document = data_util.clean_document(document, nlp)

        # current_app.logger.debug("CLEAN DOCUMENT %s" %clean_document)

        # -------- Replace text by word embeddings -------- #
        # mebddings type could be a parameter (instead of just fast text)
        document_embeddings = data_util.embeddings_representation(
            clean_document, "fasttext", nlp=nlp, language=lang
        )

        # -------- Sentence embeddings -------- #
        document_embeddings = [
            data_util.sentence_representation(
                sentence, type=self.task.parameters["type_sentence_representation"]
            )
            for sentence in document_embeddings
        ]

        # -------- Summary generation -------- #
        if self.task.parameters["ts_approach"] == "mmr":

            # -------- Lin and Bilmes -------- #
            lb_sentences = maximal_marginal_relevance(
                document,
                document_embeddings,
                lambd=2,
                r=0.6,
                budget=self.task.parameters["summary_length"],
            )  # output: [(pagerank_value, sentence_index)]

            # current_app.logger.debug("LB_SENTENCES: %s" %lb_sentences)

            summary = data_util.summary_generation(
                document,
                document_embeddings,
                lb_sentences,
                self.task.parameters["summary_length"],
                self.task.parameters["similarity_threshold"],
                self.task.parameters["type_summary"],
            )

        elif self.task.parameters["ts_approach"] == "textrank":
            # -------- TextRank -------- #
            textrank_sentences = textrank(
                document_embeddings
            )  # output: [(pagerank_value, sentence_index)]

            # current_app.logger.debug("TEXTRANK_SENTENCES: %s" %textrank_sentences)

            summary, scores = data_util.summary_generation(
                document,
                document_embeddings,
                textrank_sentences,
                self.task.parameters["summary_length"],
                self.task.parameters["similarity_threshold"],
                self.task.parameters["type_summary"],
            )

        self.scores = scores
        # current_app.logger.debug("SUMMARY: %s" %summary)
        # current_app.logger.debug("scores: %s" %scores)

        return {"summary": summary}

    async def estimate_interestingness(self):
        # TODO:
        return {"sentence_scores": self.scores}
