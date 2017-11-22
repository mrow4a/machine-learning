package mrow4a.spark.java.alg;

public final class ReservoirSampling {

    public enum Sample {
        EVICT,
        KEEP,
        REPLACE
    }

    /**
     *
     * @param M - max number of elements
     * @param t - current sample count
     * @return Sample
     */
    public static Sample sample(Integer M, Long t) {
        double ratio = (double) M / (double) t;
        if (ratio >= 1.0) {
            // t <= M
            return Sample.KEEP;
        } else if (BiasedCoin.flip(ratio)){
            // probability of t/M to replace or evict
            return Sample.REPLACE;
        } else {
            return Sample.EVICT;
        }
    }

}
