package mrow4a.spark.java.alg;

public final class EmailParser {

    public static String parse(String email) {
        String[] split = email.split("\\n\\n", 2);
        if (split.length > 1) {
            return split[1];
        }
        return split[0];
    }

}
