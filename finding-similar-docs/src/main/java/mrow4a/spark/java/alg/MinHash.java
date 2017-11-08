package mrow4a.spark.java.alg;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import mrow4a.spark.java.alg.HashFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.scalactic.Bool;
import scala.Tuple2;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.collection.Seq;

public class MinHash {

    private List<HashFunction> hashFuncions;

    public MinHash() {}

    public JavaRDD<Tuple2<String,Vector>> fit(JavaPairRDD<String, Collection<Integer>> _setIdShinglesPairs, int signatureLength) {
        this.hashFuncions = new ArrayList<>();
        for (int i = 0; i < signatureLength; i++) {
            this.hashFuncions.add(new HashFunction());
        }

        JavaRDD<Tuple2<String,Vector>> setShinglesMatrix = _setIdShinglesPairs
                .map((Function<Tuple2<String, Collection<Integer>>, Tuple2<String,Vector>>) shinglesMatrixPositions -> {
                    // Assign positions in the matrix for this column
                    ArrayList<Tuple2<Integer, Double>> shinglesMatrixBools = new ArrayList<>();
                    for (Integer shinglesMatrixPosition : shinglesMatrixPositions._2) {
                        shinglesMatrixBools.add(new Tuple2<>(
                                shinglesMatrixPosition, // Position of shingle in the matrix for this column
                                1.0) // Mark as 1 for MinHash function
                        );
                    }
                    return new Tuple2<>(
                            shinglesMatrixPositions._1,
                            Vectors.sparse(Integer.MAX_VALUE, shinglesMatrixBools)
                    );
                });

        return setShinglesMatrix;
    }
}
