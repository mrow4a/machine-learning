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

import mrow4a.spark.java.alg.EmailParser;
import mrow4a.spark.java.alg.JaccardSimilarity;
import mrow4a.spark.java.alg.MinHash;
import mrow4a.spark.java.alg.Shingling;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.SparseVector;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;

public final class FindingSimilarDocsMinHashJob {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf()
                .setAppName("JavaPrefixSpanExample")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .setMaster("local[*]").set("spark.executor.memory", "1g");
        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        // multiple = /home/mrow4a/Projects/machine-learning/finding-similar-docs/datasets/mini_newsgroups/*/**
        // single dir = /home/mrow4a/Projects/machine-learning/finding-similar-docs/datasets/mini_newsgroups/alt.atheism/*
        if (args.length < 1) {
            System.err.println();
            System.err.println("Usage: FindingSimilarDocsMinHashJob <file>");
            System.err.println();
            System.exit(1);
        }

        Instant start = Instant.now();

        /*
         * Set shingle length to 5 as it is good for emails
         */
        Integer shingleLengthEmail = 5;

        /*
         * Create shingles unique list
         */
        JavaPairRDD<String, Collection<Integer>> shingles = jsc
                /*
                 * Read directory contents to RDD with key-value, key -> filename and value -> contents
                 */
                .wholeTextFiles(args[0])
                /*
                 * Parse emails to remove headers
                 */
                .mapToPair(filenameContentsPair -> new Tuple2<>(
                        filenameContentsPair._1, // filename
                        EmailParser.parse(filenameContentsPair._2)) // shingles of contents
                )
                /*
                 * Compute shingles hashset for each document
                 */
                .mapToPair(filenameContentsPair -> new Tuple2<>(
                        filenameContentsPair._1, // filename
                        Shingling.getShingles(filenameContentsPair._2, shingleLengthEmail)) // shingles of contents
                );

        /*
         * Set signature length to 10 and compute signatures using MinHash algorithm
         */
        Integer signatureLength = 10;

        /*
         * Instantiate MinHash
         */
        MinHash mh = new MinHash(signatureLength);
        JavaRDD<Tuple2<String, Collection<Integer>>> minHashSignatures = mh
                /*
                 * Compute signatures using MinHash algorithm
                 */
                .getSignatures(shingles)
                /*
                 * Cache, otherwise we will end up with MapReduce like behaviour
                */
                .cache();


        /*
         * Set similarity for email to 0.1, which should indicate if responses or similar topic
         */
        Double similarityThresholdEmail = 0.2;

        /*
         * Combine each file with each file for similarity comparison
         */
        JavaPairRDD<Tuple2<String, Collection<Integer>>, Tuple2<String, Collection<Integer>>> uniqueFileShinglesPairs = minHashSignatures
                .cartesian(minHashSignatures)
                /*
                 * Ensure uniqueness of pairs
                 */
                .filter(setsPair -> setsPair._1._1.hashCode() < setsPair._2._1.hashCode())
                .cache();


        /*
         * Compute Jaccard similarity of signatures
         */
        JavaPairRDD<Tuple2<String, String>, Float> similarities = uniqueFileShinglesPairs
                .mapToPair(setsPair -> new Tuple2<>(
                        new Tuple2<>(setsPair._1._1, setsPair._2._1), // filename-filename pair
                        JaccardSimilarity.compute(setsPair._1._2, setsPair._2._2)
                ))
                .filter(filesSimilarityPair -> filesSimilarityPair._2 > similarityThresholdEmail)
                .cache();

        long countSim = similarities.count();
        long countUniqPairs = uniqueFileShinglesPairs.count();
        Instant end = Instant.now();

        /*
         * Print pairs of similar documents and their similarity
         */
        System.err.println();
        for (Tuple2<Tuple2<String, String>,Float> tuple : similarities.collect()) {
            System.out.println(tuple._1() + ": " + tuple._2().toString());
            //System.out.println(tuple.toString());
        }
        System.err.println();


        System.out.println("Time: "+
                Duration.between(start, end).toMillis() +" milliseconds");
        System.out.println("Found similar document pairs ["+
                countSim + "/" + countUniqPairs + "] with similarity threshold [" + similarityThresholdEmail
                + "] and shingle lenght [" + shingleLengthEmail + "] and signature lenght [" + signatureLength + "]");
        System.err.println();

        spark.stop();
    }
}
