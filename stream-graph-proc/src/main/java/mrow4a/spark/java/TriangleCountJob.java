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
import mrow4a.spark.java.alg.TriangleCount;
import mrow4a.spark.java.alg.TriestState;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.QueueInputDStream;
import scala.Tuple2;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.Queue;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;


public final class TriangleCountJob {

    public static void main(String[] args) throws Exception {
        // /home/mrow4a/Projects/machine-learning/stream-graph-proc/datasets/run
        if (args.length < 2) {
            System.err.println();
            System.err.println("Usage: StreamGraphProc <file> <checkpointdir>");
            System.err.println();
            System.exit(1);
        }

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf()
                .setAppName("StreamGraphProc")
                .setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.checkpoint(args[1]);

        // Mock stream
        JavaRDD<String> rdd = jssc.sparkContext().textFile(args[0]);
        Queue<JavaRDD<String>> rddQueue = new LinkedList<>();
        rddQueue.add(rdd);
        JavaDStream<String> lines = jssc.queueStream(rddQueue);

        // Process triangle count
        Instant start = Instant.now();
        JavaDStream<Edge> edges = EdgeParser.parse(lines);

        Integer memoryLimit = 100000;
        JavaDStream<TriestState> triangleCounts = TriangleCount.process(edges, memoryLimit);

        triangleCounts.foreachRDD(count -> {
            Integer size = count.collect().size();
            if (size > 0) {
                TriestState lastState = count.collect().get(size - 1);
                System.out.println("Current triangle count estimate: " + lastState.getTriangleCount() + " at t=" + lastState.getSeenEdges());
            }
        });

        jssc.start();
        jssc.awaitTermination();

        Instant end = Instant.now();
        System.out.println("Time: "+
                Duration.between(start, end).toMillis() +" milliseconds");
        System.out.println();
    }
}