import numpy as np
import networkx as nx
from sklearn.metrics.pairwise import cosine_similarity

def textrank(sentences):
    print("TextRank method ...")
    # Similarity matrix
    sim_mat = np.zeros([len(sentences), len(sentences)])
    for i in range(len(sentences)):
        for j in range(len(sentences)):
            if i != j:
                sim_mat[i][j] = cosine_similarity(sentences[i].reshape(1,len(sentences[i])), sentences[j].reshape(1,len(sentences[j])))[0,0]
    # Pagerank algorithm
    nx_graph = nx.from_numpy_array(sim_mat)
    scores = nx.pagerank(nx_graph)
    # Sort sentences
    ranked_sentences = sorted(((scores[i],i) for i,s in enumerate(sentences)), reverse=True)
    return ranked_sentences
