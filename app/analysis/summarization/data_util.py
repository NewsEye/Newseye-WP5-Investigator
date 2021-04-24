# -*- coding: utf-8 -*-
# Author = Elvys LINHARES PONTES
# author_email = elvyslpontes@gmail.com
# Description =
# version = 0.0.1
from flask import current_app
import spacy, os
from sklearn.metrics.pairwise import cosine_similarity
import fasttext
import os.path

# from google_drive_downloader import GoogleDriveDownloader as gdd
import gzip, shutil
import urllib.request
import numpy as np


def load_document(path):
    document = ""
    with open(path, "r") as f:
        for l in f:
            document += l
    return document


def download_decompress(filename, language):
    if not os.path.exists("data"):
        os.mkdir("data")
        current_app.logger.info("Directory 'data' created ")

    current_app.logger.info(
        "Word embeddings file does not exist.\nDownloading file cc."
        + language
        + ".300.bin.gz..."
    )
    urllib.request.urlretrieve(
        "https://dl.fbaipublicfiles.com/fasttext/vectors-crawl/cc."
        + language
        + ".300.bin.gz",
        filename + ".gz",
    )
    current_app.logger.info("Extracting file ...")
    with gzip.open(filename + ".gz", "r") as f_in, open(filename, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)
    os.remove(filename + ".gz")


def tokenizer_sentences(text, nlp, language):
    if language != "en" and language != "fr":
        try:
            nlp.add_pipe(
                nlp.create_pipe("sentencizer")
            )  # 'sentencizer' already exists in pipeline
        except ValueError:
            pass
    doc = nlp(text)
    doc = [s.text for s in doc.sents]
    return doc


def process_document(document, nlp, language):
    texts = []
    for text in document:
        text = tokenizer_sentences(text, nlp, language)
        text = [s.strip() for s in text]
        texts.append(text)
    return texts


def clean_document(document, nlp):
    clean_document = []
    for sentence in document:
        clean_sentence = []
        for token in nlp(sentence):
            # Remove: stopwords, punctuations, numbers
            if (not token.is_stop) and (not token.is_punct) and (not token.like_num):
                clean_sentence.append(token.lower_)
        clean_document.append(" ".join(clean_sentence))
    return clean_document


def embeddings_representation(document, type_embeddings, nlp, language):
    if type_embeddings == "fasttext":
        if not os.path.exists("./data/cc." + language + ".300.bin"):
            download_decompress("./data/cc." + language + ".300.bin", language)
        model = fasttext.load_model("./data/cc." + language + ".300.bin")
    # TODO: newseye
    document_embeddings = []
    for sentence in document:
        sentence_embedding = []
        for token in nlp(sentence):
            token = token.text
            if token in model:
                sentence_embedding.append(model[token])
        document_embeddings.append(sentence_embedding)
    return document_embeddings


def sentence_representation(sentence, type):
    if type == "mean":
        return sum(sentence) / (len(sentence) + 0.001)
    elif type == "sum":
        return sum(sentence)


def similar_to_summary(sentences, candidate, summary, similarity_threshold):
    for s in summary:
        similarity = cosine_similarity(
            sentences[candidate].reshape(1, len(sentences[candidate])),
            sentences[s].reshape(1, len(sentences[s])),
        )[0, 0]
        if similarity > similarity_threshold:
            return True
    return False


def summary_generation(
    document,
    sentences,
    ranked_sentences,
    summary_length,
    similarity_threshold,
    type_summary,
):
    summary = []
    scores = []
    for score, indice in ranked_sentences:
        if type_summary == "ai":
            if isinstance(sentences[indice], np.ndarray) and summary_length >= len(
                document[indice].split()
            ):
                if not similar_to_summary(
                    sentences, indice, summary, similarity_threshold
                ):
                    summary.append(indice)
                    scores.append(score)
                    summary_length -= len(document[indice].split())
        else:
            if summary_length >= len(document[indice].split()):
                summary.append(indice)
                scores.append(score)
                summary_length -= len(document[indice].split())
    return [document[i] for i in summary], scores
