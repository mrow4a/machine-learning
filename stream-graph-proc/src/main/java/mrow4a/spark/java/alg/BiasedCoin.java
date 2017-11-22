package mrow4a.spark.java.alg;

public final class BiasedCoin
{

    public static boolean flip(double d) {
        double bias = 0.5;
        if ( d >=0 && d <= 1 )  {
            bias = d;
        }

        return Math.random() < bias;
    }

}
