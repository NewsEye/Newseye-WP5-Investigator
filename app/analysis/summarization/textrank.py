import numpy as np
import networkx as nx
from sklearn.metrics.pairwise import cosine_similarity
from flask import current_app


def textrank(sentences):
    current_app.logger.info("TextRank method ...")
    current_app.logger.debug("Sentences: %s" % sentences)
    # Similarity matrix
    sim_mat = np.zeros([len(sentences), len(sentences)])
    for i in range(len(sentences)):
        for j in range(len(sentences)):
            if i != j:
                try:
                    sim_mat[i][j] = cosine_similarity(
                        sentences[i].reshape(1, len(sentences[i])),
                        sentences[j].reshape(1, len(sentences[j])),
                    )[0, 0]
                except AttributeError as e:
                    current_app.logger.debug("Unexpected sentence embedding: %s" % e)
                    sim_mat[i][j] = 0.0

    # Pagerank algorithm
    nx_graph = nx.from_numpy_array(sim_mat)
    scores = nx.pagerank(nx_graph)
    # Sort sentences
    ranked_sentences = sorted(
        ((scores[i], i) for i, s in enumerate(sentences)), reverse=True
    )
    scores_ = [s for (s, i) in ranked_sentences]
    ranked_sentences = [(s / max(scores_), i) for (s, i) in ranked_sentences]
    return ranked_sentences
