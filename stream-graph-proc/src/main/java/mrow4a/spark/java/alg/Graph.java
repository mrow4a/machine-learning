package mrow4a.spark.java.alg;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;

public class Graph implements Serializable {
    private HashMap<Integer, HashSet<Integer>> adjList;
    private HashMap<Integer, Edge> vertices;

    private Integer numEdges;

    public Graph() {
        adjList = new HashMap<>();
        vertices = new HashMap<>();
        numEdges = 0;
    }
}