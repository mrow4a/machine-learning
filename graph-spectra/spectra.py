#!/usr/bin/env python

import os
import sys
import getopt
import csv
import matplotlib.pyplot as plt
import numpy as np
from numpy.linalg import eig
from sklearn.cluster import KMeans

def print_help():
    print ('./spectra.py file_path number_of_clusters')
    print ('./spectra.py /home/ubuntu/edges.dat 4')

def print_edges(edges):
    # Convert to full matrix
    n = max(max(left, right) for left, right, weight in edges)  # Get size of matrix
    matrix = [[-1] * n for i in range(n)]
    for left, right, weight in edges:
        matrix[left - 1][right - 1] = weight  # Convert to 0-based index.

    A = np.matrix(matrix)
    plt.matshow(A)
    plt.show()

def print_affinity(edges):
    n = len(edges)
    affinity_matrix = [[0] * n for i in range(n)]
    for i in range(0, n):
        # Create affinity matrix
        for j in range(0, n):
            affinity = 0
            if i != j:
                pointI= edges[i]
                pointJ = edges[j]
                dist = np.sqrt(np.power((pointI[0] - pointJ[0]), 2) + np.power((pointI[1] - pointJ[1]), 2))
                sigma = 1
                affinity = np.exp(-1 * np.power(dist, 2) / (2*np.power(sigma, 2)))

            affinity_matrix[i][j] = affinity
    A = np.matrix(affinity_matrix)
    plt.matshow(A)
    plt.show()

def main(argv):
    try:
        opts, args = getopt.getopt(argv, "", [])
    except getopt.GetoptError:
        print_help()
        sys.exit(2)

    if (len(args)) < 2:
        print_help()
        sys.exit(2)
    path = args[0]
    number_of_clusters = int(args[1])

    # Read file
    with open(path, 'r') as f:
        edges = [map(int, row) for row in csv.reader(f, delimiter=',')]

    import networkx as nx
    G = nx.Graph()

    # Sanitize data
    for i in range(0, len(edges)):
        if len(edges[i]) < 2:
            print "Wrong data format"
            exit(2)
        if len(edges[i]) == 2:
            edges[i].append(1)

        G.add_edge(edges[i][0], edges[i][1])

    # matrix = nx.adjacency_matrix(G).todense()
    # plt.matshow(matrix)
    # plt.show()

    # Get Laplacian from affinity matrix
    # Get Eigenvectors in ascending order
    laplacian = nx.laplacian_matrix(G).todense()
    eigenvalues, eigenvectors = eig(laplacian)
    idx = eigenvalues.argsort()
    eigenvalues = eigenvalues[idx]

    # Get Normalized Laplacian from affinity matrix
    # Get Eigenvectors in descending order (the normalized (unit length) eigenvectors)
    norm_laplacian = nx.normalized_laplacian_matrix(G).todense()
    norm_eigenvalues, norm_eigenvectors = eig(norm_laplacian)
    idx = norm_eigenvalues.argsort()
    norm_eigenvalues = norm_eigenvalues[idx]
    norm_eigenvectors = norm_eigenvectors[:, idx]

    plt.plot(eigenvalues, 'ro')
    plt.suptitle('Not-normed eigenvalues')
    plt.show()
    plt.plot(eigenvalues[0:16], 'ro')
    plt.suptitle('Not-normed first 20 eigenvalues')
    plt.show()
    plt.plot(norm_eigenvalues, 'ro')
    plt.suptitle('Normed eigenvalues')
    for i in range(0, number_of_clusters):
        plt.plot(norm_eigenvectors[:,i])
    plt.suptitle('Normed Largest Eigenvectors')
    plt.show()

    # Get k largest eigenvectors by its eigenvalues
    n = len(eigenvalues)
    eigenvector_points = [[0] * number_of_clusters for i in range(n)]
    for k in range(0, number_of_clusters):
        eigenvector = norm_eigenvectors[:,k].tolist()
        for i in range(0, len(eigenvector)):
            real_imaginary_parts = eigenvector[i]
            real_part = real_imaginary_parts[0]
            eigenvector_points[i][k] = real_part

    kmeans_model_labels = KMeans(n_clusters=number_of_clusters).fit_predict(eigenvector_points)

    for i in range(0, len(edges)):
        v1 = edges[i][0]
        v2 = edges[i][1]
        v1cluster = kmeans_model_labels[v1-1]
        v2cluster = kmeans_model_labels[v2-1]
        if (v1cluster == v2cluster):
            edges[i][2] = v1cluster

    print kmeans_model_labels

    print_edges(edges)


if __name__ == '__main__':
    main(sys.argv[1:])
