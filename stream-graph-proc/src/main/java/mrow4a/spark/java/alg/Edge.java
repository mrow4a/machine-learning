package mrow4a.spark.java.alg;

import java.io.Serializable;

public class Edge implements Serializable {
    public Integer left;
    public Integer right;

    Edge(Integer left, Integer right) {
        this.left = left;
        this.right = right;
    }


    public String toString() {
        return "("+left.toString()+","+right.toString()+")";
    }
}
