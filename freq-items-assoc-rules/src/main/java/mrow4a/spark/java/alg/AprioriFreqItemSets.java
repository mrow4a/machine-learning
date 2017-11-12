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
         * Initialize broadcast
         */
        Broadcast<HashSet<String>> broadcast = jsc.broadcast(new HashSet<>());
        boolean converged = false;
        int stageNumber = 1;

        JavaPairRDD<List<String>, Integer> frequentItems = jsc.parallelizePairs(new ArrayList<>());
        while (!converged) {
            int currentStage = stageNumber;
            Broadcast<HashSet<String>> currentBroadcast = broadcast;
            JavaPairRDD<List<String>, Integer> prunedCandidates = baskets
                    .flatMap((FlatMapFunction<List<String>, List<String>>) items -> {
                        // Get all possible combinations of items for this basket which have specific length
                        List<List<String>> combinations = Combination
                                .getSubArraysOfLength(items, currentStage);

                        // Retrieve candidates from last stage
                        HashSet<String> lastStagePrunedCandidates= currentBroadcast.value();

                        // Contruct list of candidates
                        List<List<String>> candidatesItems = new ArrayList<>();
                        for(List<String> combination : combinations) {
                            boolean isFrequent = true;
                            if (currentStage != 1) {
                                // In the first stage there are no subcombinations to check
                                // In other stages, for a candidate to be a frequent itemset, all its subsets
                                // must be frequent, thus check subcombinations
                                List<List<String>> subCombinations = Combination
                                        .getSubArraysOfLength(combination, currentStage - 1);

                                // Check all subcombinations, if at least one of the is not frequent,
                                // parent combination is not frequent
                                for(List<String> subCombination : subCombinations) {
                                    String serialSubCombination = subCombination.toString();
                                    if (!lastStagePrunedCandidates.contains(serialSubCombination)) {
                                        isFrequent = false;
                                    }
                                }

                            }

                            if (isFrequent) {
                                candidatesItems.add(combination);
                            }
                        }

                        return candidatesItems.iterator();
                    })
                    // Group the items and count number of its occurences
                    .mapToPair(s -> new Tuple2<>(s, 1))
                    .reduceByKey((i1, i2) -> i1 + i2)
                    // Filter by support threshold
                    .filter(itemCount -> filterBySupport(itemCount._2, basketsCount, supportThreshold))
                    .cache();

            converged = prunedCandidates.isEmpty();
            if (!converged) {
                // Add pruned, frequent candidates to frequent items list
                frequentItems = frequentItems
                        .union(prunedCandidates);

                // Broadcast candidates from this stage to next stage
                List<String> serializedFrequentItems = prunedCandidates
                        .map(candidateCountPair -> candidateCountPair._1.toString())
                        .collect();
                broadcast = jsc.broadcast(new HashSet<>(serializedFrequentItems));

                // Increase stage index
                stageNumber++;
            }
        }



        return frequentItems;
    }

    private static boolean filterBySupport(long itemCount, long totalCount, double supportThreshold) {
        double support = (double) itemCount / (double) totalCount;
        return support >= supportThreshold;
    }

}
