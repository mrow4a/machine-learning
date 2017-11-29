#!/usr/bin/env python

import os
import sys
import getopt
import csv
import matplotlib.pyplot as plt
import numpy as np

def print_help():
    print ('./spectra.py filepath')

def main(argv):
    try:
        opts, args = getopt.getopt(argv, "", [])
    except getopt.GetoptError:
        print_help()
        sys.exit(2)

    if (len(args)) < 1:
        print_help()
        sys.exit(2)
    path = args[0]

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

    # Convert to full matrix
    n = max(max(left, right) for left, right, weight in edges)  # Get size of matrix
    matrix = [[0] * n for i in range(n)]
    for left, right, weight in edges:
        matrix[left - 1][right - 1] = weight  # Convert to 0-based index.

    A = np.matrix(matrix)
    # Print full matrix

    # Display matrix
    plt.matshow(A)

    plt.show()


if __name__ == '__main__':
    main(sys.argv[1:])
