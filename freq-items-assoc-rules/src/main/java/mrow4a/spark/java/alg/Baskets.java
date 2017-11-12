package mrow4a.spark.java.alg;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class Baskets {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static List<String> parse(String text) {
        return Arrays.asList(SPACE.split(text));
    }
}
