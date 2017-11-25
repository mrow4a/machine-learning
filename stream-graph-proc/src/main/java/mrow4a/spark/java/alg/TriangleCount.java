package mrow4a.spark.java.alg;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import scala.Serializable;
import scala.Tuple2;

import java.util.Arrays;


class TriangleCountState implements Serializable {

    ReservoirGraph graph;
    Integer triangleCount;

    TriangleCountState(Integer memory) {
        this.graph = new ReservoirGraph(memory);
        this.triangleCount = 0;
    }

    Boolean print() {
        System.out.println();
        //this.graph.print();
        System.out.println("Current triangle count: "+this.triangleCount);
        return true;
    }
}

public final class TriangleCount {

    public static void process(JavaDStream<Edge> edges, Integer memoryLimit) {
        JavaPairRDD<Integer, TriangleCountState> initialRDD = JavaSparkContext
                .fromSparkContext(edges.context().sparkContext())
                .parallelizePairs(Arrays.asList(new Tuple2<>(1, new TriangleCountState(memoryLimit))));

        edges.mapToPair(edge -> new Tuple2<>(1, edge))
                .mapWithState(
                        StateSpec.function(new Function3<Integer, Optional<Edge>, State<TriangleCountState>, Integer>() {

                            @Override
                            public Integer call(Integer key, Optional<Edge> curEdge, State<TriangleCountState> state) {
                                TriangleCountState tcState = state.get();

                                TriangleCount.updateState(tcState, curEdge.get());

                                // We care only about state, dont output anything
                                return null;
                            }
                        }).initialState(initialRDD).numPartitions(1)
                )
                .stateSnapshots()
                .map( state -> new Tuple2<>(state._2.graph.getSeenEdges(), TriangleCount.getEstimate(state._2, state._2.triangleCount)))
                .foreachRDD(count ->  {
                    Tuple2<Long, Integer> triangleCount = count.collect().get(count.collect().size()-1);
                    System.out.println("Current triangle count estimate: "+triangleCount._2+" at t="+triangleCount._1);
                });
    }

    private static void updateState(TriangleCountState state, Edge currentEdge) {
        Edge evictedEdge = state.graph.addEdgeWithEvict(currentEdge);
        if (evictedEdge == null) {
            // Edge has been added without eviction of other edge
            state.triangleCount += state.graph.getSharedNeighborhood(currentEdge).size();
        } else if (evictedEdge != currentEdge) {
            // Edge has been added with eviction of other edge
            // Substract from evicted edge shared nodes, and add to current edge
            state.triangleCount -= state.graph.getSharedNeighborhood(evictedEdge).size();
            state.triangleCount += state.graph.getSharedNeighborhood(currentEdge).size();
        }
    }

    private static Integer getEstimate(TriangleCountState state, Integer triangleCount) {
        Long seenEdges = state.graph.getSeenEdges();
        Integer maxEdges = state.graph.getMaxEdges();
        if (state.graph.getSeenEdges() < state.graph.getMaxEdges()) {
            return triangleCount;
        } else {
            return Math.max(1, (int)((double) triangleCount * ((double) (seenEdges*(seenEdges-1)*(seenEdges-2)) / (double) (maxEdges*(maxEdges-1)*(maxEdges-2))) ) );
        }
    }
}

//                edges.mapToPair(edge -> new Tuple2<>(1, edge))
//                        .mapWithState()
//                .updateStateByKey(
//                (Function2<List<Edge>, Optional<TriangleCountState>, Optional<TriangleCountState>>) (values, state) -> {
//                    TriangleCountState tcState = state.or(new TriangleCountState(memoryLimit));
//                    for(Edge edge : values) {
//                        TriangleCount.updateState(tcState, edge);
//                    }
//                    return Optional.of(tcState);
//                })
//                .map(state -> state._2().print())
//                .foreachRDD(rdd -> rdd.collect());
;