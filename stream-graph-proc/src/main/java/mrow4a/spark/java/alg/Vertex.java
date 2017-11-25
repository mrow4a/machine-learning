package mrow4a.spark.java.alg;

import java.io.Serializable;

public class Vertex implements Serializable {
    public Integer node;

    Vertex(Integer node) {
        this.node = node;
    }

    public String toString() {
        return this.node.toString();
    }
}
