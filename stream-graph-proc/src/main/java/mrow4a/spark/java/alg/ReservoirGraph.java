package mrow4a.spark.java.alg;

import mrow4a.spark.java.alg.ReservoirSampling;
import mrow4a.spark.java.alg.ReservoirSampling.Sample;
import scala.Array;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

public class ReservoirGraph implements Serializable {
    private HashMap<Integer, Edge> edges;
    private HashMap<String, HashSet<String>> adjMatr;

    private Integer numEdges;
    private Long seenEdges;
    private Random rand = new Random();

    ReservoirGraph(Integer numEdges) {
        this.edges = new HashMap<>();
        this.adjMatr = new HashMap<>();
        this.numEdges = numEdges;
        this.seenEdges = 0L;
    }

    /**
     * Processes the edge and
     * return evicted edge if replace was issued,
     * current edge if dropped,
     * or null in case of no eviction
     *
     * @param edge - edge to be processed
     * @return Edge -
     */
    Edge addEdgeWithEvict(Edge edge) {
        this.seenEdges++;
        Edge evictedEdge = null;
        Sample sampleTech = ReservoirSampling.sample(numEdges, seenEdges);
        if (sampleTech == Sample.KEEP) {
            // Fill the verticies
            addEdge(edge, this.edges.size());
        } else if (sampleTech == Sample.REPLACE) {
            // Remove random edge
            Integer index = this.rand.nextInt(numEdges);
            evictedEdge = removeEdge(index);

            // Replace with new one
            addEdge(edge, index);
        } else {
            // Drop current edge
            evictedEdge = edge;
        }

        return evictedEdge;
    }

    /**
     * Get shared neighborhood
     *
     * @param edge - edge to check
     * @return List<String>
     */
    List<String> getSharedNeighborhood(Edge edge) {
        ArrayList<String> sharedNodes = new ArrayList<>();

        HashSet<String> leftNeigh = getAdj(edge.left, false);
        HashSet<String> rightNeigh = getAdj(edge.right, false);

        HashSet<String> checker = leftNeigh.size() > rightNeigh.size() ? leftNeigh : rightNeigh;
        HashSet<String> map = leftNeigh.size() <= rightNeigh.size() ? leftNeigh : rightNeigh;

        for (String node: checker){
            if (map.contains(node)) {
                sharedNodes.add(node);
            }
        }

        return sharedNodes;
    }

    private void addEdge(Edge edge, Integer index) {
        this.edges.put(index, edge);

        HashSet<String> adjLeft = getAdj(edge.left, true);
        adjLeft.add(edge.right.toString());

        HashSet<String> adjRight = getAdj(edge.right, true);
        adjRight.add(edge.left.toString());

        this.adjMatr.put(edge.left.toString(), adjLeft);

        this.adjMatr.put(edge.right.toString(), adjRight);
    }

    private Edge removeEdge(Integer index) {
        Edge removedEdge = this.edges.remove(index);

        HashSet<String> adjLeft = getAdj(removedEdge.left, true);
        adjLeft.remove(removedEdge.right.toString());

        HashSet<String> adjRight = getAdj(removedEdge.right, true);
        adjRight.remove(removedEdge.left.toString());

        this.adjMatr.put(removedEdge.left.toString(), adjLeft);

        this.adjMatr.put(removedEdge.right.toString(), adjRight);

        return removedEdge;
    }

    private HashSet<String> getAdj(Vertex node, Boolean withDelete) {
        if (this.adjMatr.containsKey(node.toString())) {
            if (withDelete) {
                return this.adjMatr.remove(node.toString());
            }
            return this.adjMatr.get(node.toString());
        } else {
            return new HashSet<>();
        }
    }

    void print() {
        System.out.println();
        System.out.println();
        for (String name: this.adjMatr.keySet()){
            String value = this.adjMatr.get(name).toString();
            System.out.println(name + " " + value);
        }
    }
}