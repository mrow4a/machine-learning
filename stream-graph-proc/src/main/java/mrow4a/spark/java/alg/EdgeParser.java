package mrow4a.spark.java.alg;

import org.apache.spark.streaming.api.java.JavaDStream;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;

public final class EdgeParser {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static JavaDStream<Edge> parse(JavaDStream<String> textStream) {
        return textStream
                .map(line -> Arrays.asList(SPACE.split(line)))
                .filter(line -> (line.size() == 2))
                .filter(rawArray -> rawArray.get(0).matches("^-?\\d+$") && rawArray.get(1).matches("^-?\\d+$"))
                .map(intArray ->
                        new Edge(
                                Integer.parseInt(intArray.get(0)),
                                Integer.parseInt(intArray.get(1))
                        )
                );
    }

}
