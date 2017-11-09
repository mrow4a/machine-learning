package mrow4a.spark.java.alg;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Vector;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.linalg.Vectors;

public class MinHash {

    private final int signatureLength;

    public MinHash(int signatureLength) {
        this.signatureLength = signatureLength;
    }

    public JavaRDD<Tuple2<String, Collection<Integer>>> getSignatures(JavaPairRDD<String, Collection<Integer>> _setIdShinglesPairs) {
        Signature signature = new Signature(signatureLength);

        return _setIdShinglesPairs
                .filter(shinglesMatrixPositions -> !shinglesMatrixPositions._2.isEmpty())
                .map((Function<Tuple2<String, Collection<Integer>>, Tuple2<String,SparseVector>>) shinglesMatrixPositions -> {
                    // Assign positions in the matrix for this column
                    ArrayList<Tuple2<Integer, Double>> shinglesMatrixBools = new ArrayList<>();
                    for (Integer shinglesMatrixPosition : shinglesMatrixPositions._2) {
                        shinglesMatrixBools.add(new Tuple2<>(
                                shinglesMatrixPosition, // Position of shingle in the matrix for this column
                                1.0) // Mark as 1 for MinHash function
                        );
                    }

                    return new Tuple2<>(
                            shinglesMatrixPositions._1, // filename
                            Vectors.sparse( // sparse shingles matrix
                                    Integer.MAX_VALUE, // Max int artificial vector size is required since shingles can be any integer
                                    shinglesMatrixBools // positions of shingles
                            ).toSparse()
                    );
                })
                .map((Function<Tuple2<String, SparseVector>, Tuple2<String,Collection<Integer>>>) shinglesMatrixPositions -> {
                    // Assign positions in the matrix for this column
                    int[] setShingles = shinglesMatrixPositions._2.indices();

                    return new Tuple2<>(
                            shinglesMatrixPositions._1,
                            signature.get(setShingles)
                    );
                });
    }
}
