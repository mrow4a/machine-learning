/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mrow4a.spark.java;

import mrow4a.spark.java.alg.Edge;
import mrow4a.spark.java.alg.EdgeParser;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import java.time.Duration;
import java.time.Instant;

public final class TriangleCountJob {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf()
                .setAppName("StreamGraphProc")
                .setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // /home/mrow4a/Projects/machine-learning/stream-graph-proc/datasets/run
        if (args.length < 1) {
            System.err.println();
            System.err.println("Usage: StreamGraphProc <file>");
            System.err.println();
            System.exit(1);
        }

        Instant start = Instant.now();

        JavaDStream<String> lines = jssc.textFileStream(args[0]);

        JavaDStream<Edge> edges = EdgeParser.parse(lines);
        edges.print();

//        System.out.println();
//
//        System.out.println("Association rules detected: ");
//        for (Tuple2<Tuple2<List<String>, List<String>>, Double> tuple : associationRulesCollect) {
//            System.out.println(tuple._1._1 + " -> " + tuple._1._2 + " with confidence " + tuple._2.toString());
//        }
//        System.out.println();

        jssc.start();
        jssc.awaitTermination();

        Instant end = Instant.now();
        System.out.println("Time: "+
                Duration.between(start, end).toMillis() +" milliseconds");
        System.out.println();
    }
}