package mrow4a.spark.java.alg;

import java.io.Serializable;

public class Edge implements Serializable {
    public Vertex left;
    public Vertex right;

    Edge(Integer left, Integer right) {
        this.left = new Vertex(left);
        this.right = new Vertex(right);
    }


    public String toString() {
        return "("+left.toString()+","+right.toString()+")";
    }
}
