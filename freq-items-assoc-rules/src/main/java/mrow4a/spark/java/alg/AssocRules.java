package mrow4a.spark.java.alg;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset;
import scala.Tuple2;

import java.util.List;

public final class AssocRules {

    public static JavaPairRDD<Tuple2<List<String>, List<String>>, Double> get(JavaPairRDD<List<String>, Integer> _frequentItemsetsWithSupports, double confidenceThreshold) {
        // Convert to frequent itemset object
        JavaRDD<FreqItemset<String>> frequentItemsetsWithSupports = _frequentItemsetsWithSupports
                .map((Function<Tuple2<List<String>, Integer>,FreqItemset<String>>) itemsetWithSupport -> {
                    List<String> itemset = itemsetWithSupport._1;
                    long support = itemsetWithSupport._2;
                    return new FreqItemset<>(
                            itemset.toArray(new String[itemset.size()]),
                            support
                            );
                });

        // Generate association rules
        return new AssociationRules()
                .setMinConfidence(confidenceThreshold)
                .run(frequentItemsetsWithSupports)
                .mapToPair((PairFunction<AssociationRules.Rule<String>,Tuple2<List<String>, List<String>>, Double>) assocRule -> {
                    List<String> antecedent = assocRule.javaAntecedent();
                    List<String> consequent = assocRule.javaConsequent();
                    Double confidence = assocRule.confidence();

                    return new Tuple2<>(new Tuple2<>(antecedent, consequent), confidence);
                });
    }

}
