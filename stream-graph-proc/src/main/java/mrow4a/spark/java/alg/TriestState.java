package mrow4a.spark.java.alg;

import scala.Serializable;

public class TriestState implements Serializable {

    Long seenEdges;
    Integer triangleCount;

    TriestState(Long seenEdges, Integer triangleCount) {
        this.seenEdges = seenEdges;
        this.triangleCount = triangleCount;
    }

    public Long getSeenEdges() {
        return seenEdges;
    }

    public Integer getTriangleCount() {
        return triangleCount;
    }
}
