package se.kth.spark.lab1.task2

import se.kth.spark.lab1._
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.Pipeline
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setMaster("local[*]").set("spark.executor.memory", "1g")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val filePath = "src/main/resources/millionsong.txt"
    val rdd = spark.sparkContext.textFile(filePath)
//
//    //Step1: tokenize each row
//    val regexTokenizer = new RegexTokenizer()
//      .setInputCol(???)
//      .setOutputCol(???)
//      .setPattern(???)
//
//    //Step2: transform with tokenizer and show 5 rows
//    ???
//
//    //Step3: transform array of tokens to a vector of tokens (use our ArrayToVector)
//    val arr2Vect = new Array2Vector()
//    ???
//
//    //Step4: extract the label(year) into a new column
//    val lSlicer = ???
//
//    //Step5: convert type of the label from vector to double (use our Vector2Double)
//    val v2d = new Vector2DoubleUDF(???)
//    ???
//    //Step6: shift all labels by the value of minimum label such that the value of the smallest becomes 0 (use our DoubleUDF)
//    val lShifter = new DoubleUDF(???)
//    ???
//    //Step7: extract just the 3 first features in a new vector column
//    val fSlicer = ???
//
//    //Step8: put everything together in a pipeline
//    val pipeline = new Pipeline().setStages(???)
//
//    //Step9: generate model by fitting the rawDf into the pipeline
//    val pipelineModel = pipeline.fit(rawDF)
//
//    //Step10: transform data with the model - do predictions
//    ???
//
//    //Step11: drop all columns from the dataframe other than label and features
//    ???
  }
}