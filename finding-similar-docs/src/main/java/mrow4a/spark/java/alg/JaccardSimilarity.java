package mrow4a.spark.java.alg;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

public final class JaccardSimilarity {

    public static Float compute(Collection<Integer> set1, Collection<Integer> set2) {
        int intersectionSize = 0;
        HashSet<Integer> sethash;
        ArrayList<Integer> list;
        if (set1.size() > set2.size()) {
            sethash = new HashSet<>(set1);
            list = new ArrayList<>(set2);
        } else {
            sethash = new HashSet<>(set2);
            list = new ArrayList<>(set1);
        }

        for(Integer element : list) {
            if (sethash.contains(element)) {
                intersectionSize++;
            }
        }

        int unionSize = set1.size() + set2.size() - intersectionSize;
        return ( (float) intersectionSize / (float) unionSize );
    }

}
