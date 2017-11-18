package mrow4a.spark.java.alg;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import mrow4a.spark.java.alg.Combination;
import scala.Tuple2;

import java.util.*;

public final class AprioriFreqItemSets {

    public static JavaPairRDD<List<String>, Integer> get(JavaRDD<List<String>> _baskets, double supportThreshold) {
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(_baskets.context());
        /*
         * Cache baskets
         */
        JavaRDD<List<String>> baskets = _baskets.cache();

        /*
         * Get total number of baskets
         */
        long basketsCount = baskets.count();

        /*
         * Initialize broadcast of frequent itemsets hashset
         */
        Broadcast<HashSet<String>> freqItemsetsHashSetBroadcast = jsc.broadcast(new HashSet<>());
        JavaPairRDD<List<String>, Integer> frequentItems = jsc.parallelizePairs(new ArrayList<>());

        /*
         * Initialize stages and loop. Loop until there are no more candidates to create new stage
         */
        boolean finished = false;
        int stageNumber = 1;
        while (!finished) {
            int currentCombinationLength = stageNumber;
            Broadcast<HashSet<String>> lastFreqItemsetsHashSetBroadcast = freqItemsetsHashSetBroadcast;

            // Get candidates for current stage
            JavaPairRDD<List<String>, Integer> currStageCandidates = baskets
                    // In each of the baskets, execute the map function to get candidates of length <stageNumber>
                    .flatMap((FlatMapFunction<List<String>, List<String>>) basket -> {

                        // Retrieve candidates from last stage - using broadcasted hashset of frequent items
                        HashSet<String> lastFreqItemsetsHashSet= lastFreqItemsetsHashSetBroadcast.value();

                        // Set valid subcombinations to frequent items and required combintion lenth to currentCombinationLength
                        Combination combiner = new Combination(
                                lastFreqItemsetsHashSet, // valid subcombinations
                                currentCombinationLength // required combination length
                        );

                        // Get all and valid possible combinations of items for this basket which have specific length
                        // E.g. <a,b,c> -> <ab,ac,bc> for length 2
                        // E.g. <a,b,c> -> <abc> for length 3
                        List<List<String>> combinations = combiner
                                .getSubArrays(basket);

                        return combinations.iterator();
                    })
                    // Group the items and count number of its occurences
                    .mapToPair(s -> new Tuple2<>(s, 1))
                    .reduceByKey((i1, i2) -> i1 + i2)
                    // Filter by support threshold
                    .filter(itemCount -> filterBySupport(itemCount._2, basketsCount, supportThreshold))
                    .cache();

            // Add pruned, frequent candidates to frequent items list
            frequentItems = frequentItems
                    .union(currStageCandidates);

            // Broadcast candidates from this stage to next stage
            List<String> serializedFrequentItems = currStageCandidates
                    .map(candidateCountPair -> candidateCountPair._1.toString())
                    .collect();
            freqItemsetsHashSetBroadcast = jsc.broadcast(new HashSet<>(serializedFrequentItems));

            // Increase stage index
            stageNumber++;

            // If there are no more candidates, finish
            finished = currStageCandidates.isEmpty();
        }

        return frequentItems;
    }

    private static boolean filterBySupport(long itemCount, long totalCount, double supportThreshold) {
        double support = (double) itemCount / (double) totalCount;
        return support >= supportThreshold;
    }

}
