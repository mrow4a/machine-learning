package mrow4a.spark.java.alg;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import scala.Tuple2;

import java.util.Arrays;

public final class TriangleCount {

    public static JavaDStream<TriestState> process(JavaDStream<Edge> edges, Integer memoryLimit) {
        JavaPairRDD<Integer, ReservoirGraph> initialRDD = JavaSparkContext
                .fromSparkContext(edges.context().sparkContext())
                .parallelizePairs(Arrays.asList(new Tuple2<>(1, new ReservoirGraph(memoryLimit))));

        return edges.mapToPair(edge -> new Tuple2<>(1, edge))
                .mapWithState(StateSpec.function(new Function3<Integer, Optional<Edge>, State<ReservoirGraph>, Integer>() {

                            @Override
                            public Integer call(Integer key, Optional<Edge> curEdge, State<ReservoirGraph> state) {
                                ReservoirGraph tcState = state.get();

                                // Return local triangle count
                                return TriangleCount.getLocalTriangleCount(tcState, curEdge.get());
                            }
                        }).initialState(initialRDD).numPartitions(1)
                )
                .map(localCount -> new Tuple2<>(localCount, 1L))
                .reduce(new Function2<Tuple2<Integer, Long>, Tuple2<Integer, Long>, Tuple2<Integer, Long>>() {
                            @Override
                            public Tuple2<Integer, Long> call(Tuple2<Integer, Long> seenCountPair1, Tuple2<Integer, Long> seenCountPair2) throws Exception {
                                Long seenEdges = seenCountPair1._2 + seenCountPair2._2;
                                Integer triangleCount = seenCountPair1._1 + seenCountPair2._1;
                                return new Tuple2<>(triangleCount, seenEdges);
                            }
                })
                .mapToPair(result -> new Tuple2<>(1, result))
                .mapWithState(StateSpec.function(new Function3<Integer, Optional<Tuple2<Integer, Long>>, State<Tuple2<Integer, Long>>, TriestState>() {

                            @Override
                            public TriestState call(Integer key, Optional<Tuple2<Integer, Long>> result, State<Tuple2<Integer, Long>> state) {
                                Tuple2<Integer, Long> lastCountSeen = (state.exists() ? state.get() : new Tuple2<>(0, 0L));
                                Tuple2<Integer, Long> newCountSeen = result.get();

                                Integer triangleCount = lastCountSeen._1 + newCountSeen._1;
                                Long seenEdges = lastCountSeen._2 + newCountSeen._2;
                                Integer triangleCountEstimate = TriangleCount.getEstimate(seenEdges, memoryLimit, triangleCount);

                                return new TriestState(
                                        seenEdges,
                                        triangleCountEstimate
                                );
                            }
                        }).numPartitions(1)
                );
    }

    private static Integer getLocalTriangleCount(ReservoirGraph graph, Edge currentEdge) {
        Edge evictedEdge = graph.addEdgeWithEvict(currentEdge);
        Integer localTriangleCount = 0;
        if (evictedEdge == null) {
            // Edge has been added without eviction of other edge
            localTriangleCount += graph.getSharedNeighborhood(currentEdge).size();
        } else if (evictedEdge != currentEdge) {
            // Edge has been added with eviction of other edge
            // Substract from evicted edge shared nodes, and add to current edge
            localTriangleCount -= graph.getSharedNeighborhood(evictedEdge).size();
            localTriangleCount += graph.getSharedNeighborhood(currentEdge).size();
        }

        return localTriangleCount;
    }

    private static  Integer getEstimate(Long seenEdges, Integer maxEdges, Integer triangleCount) {
        Double estimate = ((double) seenEdges / (double) maxEdges) * ((double) (seenEdges-1) / (double) (maxEdges-1)) * ((double) (seenEdges-2) / (double) (maxEdges-2));
        return (int) (triangleCount * Math.max(1, estimate));
    }
}