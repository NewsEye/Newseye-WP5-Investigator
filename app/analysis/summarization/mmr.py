import app.analysis.summarization.data_util as data_util
from math import *
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from flask import current_app


def mmr(sentences_emb, summary_indices, document_indices, lambd):
    weights = 0.0
    for d in document_indices:
        for s in summary_indices:
            if not np.array_equal(sentences_emb[d], sentences_emb[s]):
                weights += cosine_similarity(
                    sentences_emb[d].reshape(1, len(sentences_emb[d])),
                    sentences_emb[s].reshape(1, len(sentences_emb[s])),
                )[0, 0]

    penalty = 0.0
    for s1 in summary_indices:
        for s2 in summary_indices:
            if not np.array_equal(sentences_emb[s1], sentences_emb[s2]):
                penalty += cosine_similarity(
                    sentences_emb[s1].reshape(1, len(sentences_emb[s1])),
                    sentences_emb[s2].reshape(1, len(sentences_emb[s2])),
                )[0, 0]

    return lambd * weights - (1 - lambd) * penalty


def maximal_marginal_relevance(sentences, sentences_emb, lambd=2, r=0.6, budget=400):
    current_app.logger.info("Maximal Marginal Relevance ...")
    # All sentences together
    document = [i for i in range(len(sentences))]
    G = []
    U = document[:]
    # Best summary
    while len(U) > 0:
        sentence, value, valuer = "", -1000000.0, -1000000.0
        for l in U:
            v = mmr(sentences_emb, G + [l], document, lambd) - mmr(
                sentences_emb, G, document, lambd
            )
            vr = v / (len(sentences[l].split()) ** r)
            if vr > valuer:
                value = v
                valuer = vr
                sentence = l
        if (
            sum([len(sentences[i].split()) for i in G])
            + len(sentences[sentence].split())
            <= budget
            and value > 0
        ):
            G = G + [sentence]
        U.remove(sentence)
    # Best singleton
    singleton, values = 0, -9999999999
    for s in document:
        if len(sentences[s].split()) <= budget:
            v = mmr(sentences_emb, [s], document, lambd)
            if v > values:
                values = v
                singleton = s

    if mmr(sentences_emb, G, document, lambd) > values:
        return [(1.0 / (pos + 1), indice) for pos, indice in enumerate(G)] + [
            (1.0 / (10 * len(G)), indice)
            for pos, indice in enumerate(document)
            if indice not in G
        ]
    else:
        return [(1.0, singleton)] + [
            (1.0 / (10 * len(G)), indice)
            for pos, indice in enumerate(document)
            if indice != singleton
        ]
