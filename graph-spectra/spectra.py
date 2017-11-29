#!/usr/bin/env python

import os
import sys
import getopt
import csv
import matplotlib.pyplot as plt
import numpy as np
from scipy.sparse import csgraph
from numpy import linalg as LA
from sklearn.cluster import KMeans

def print_help():
    print ('./spectra.py file_path number_of_clusters')
    print ('./spectra.py /home/ubuntu/edges.dat 4')

def print_edges(edges):
    # Convert to full matrix
    n = max(max(left, right) for left, right, weight in edges)  # Get size of matrix
    matrix = [[0] * n for i in range(n)]
    for left, right, weight in edges:
        matrix[left - 1][right - 1] = weight  # Convert to 0-based index.

    A = np.matrix(matrix)
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

    # Sanitize data
    for i in range(0, len(edges)):
        if len(edges[i]) != 2 and len(edges[i]) != 3:
            print "Wrong data format"
            exit(2)

        if len(edges[i]) == 2:
            edges[i].append(1)

    # Print edges matrix
    #print_edges(edges)

    # Start spectral clustering - https://calculatedcontent.com/2012/10/09/spectral-clustering/

    # Calculate affinity matrix and diagonal
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

    # Get Normalized Laplacian from affinity matrix
    laplacian = csgraph.laplacian(np.matrix(affinity_matrix), normed=True)

    # Get Eigenvectors in ascending order (the normalized (unit length) eigenvectors)
    eigenvalues, norm_eigenvectors = LA.eigh(laplacian)

    # Get k largest eigenvectors by its eigenvalues
    kth_largest_eigenvectors = norm_eigenvectors[len(norm_eigenvectors)-number_of_clusters-1:len(norm_eigenvectors)-1]

    eigenvector_points = [[0] * 4 for i in range(n)]
    for k in range(0, len(kth_largest_eigenvectors)):
        eigenvector = kth_largest_eigenvectors[k]
        for i in range(0, len(eigenvector)):
            eigenvector_points[i][k] = eigenvector[i]

    kmeans_model_labels = KMeans(n_clusters=number_of_clusters).fit(eigenvector_points).labels_

    for i in range(0, len(kmeans_model_labels)):
        edges[i][2] = kmeans_model_labels[i]

    print_edges(edges)


if __name__ == '__main__':
    main(sys.argv[1:])
