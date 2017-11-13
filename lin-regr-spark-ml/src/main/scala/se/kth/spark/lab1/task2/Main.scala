package se.kth.spark.lab1.task2

import se.kth.spark.lab1.{Array2Vector, DoubleUDF, Obs, Vector2DoubleUDF}
import org.apache.spark.ml.feature.{RegexTokenizer, VectorSlicer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions.min

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
    val songsDf = spark.sparkContext.textFile(filePath)
      .toDF("record")
    songsDf.show(5)

    //Step1: tokenize each row
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("record")
      .setOutputCol("labels")
      .setPattern(",")

    //Step2: transform with tokenizer and show 5 rows
    val tokenized = regexTokenizer.transform(songsDf)
    tokenized.show(5)

    //Step3: transform array of tokens to a vector of tokens (use our ArrayToVector)
    val arr2Vect = new Array2Vector()
      .setInputCol("labels")
      .setOutputCol("vector")
    // arr2Vect.transform(tokenized).show(5)

    //Step4: extract the label(year) into a new column
    val lSlicer = new VectorSlicer()
      .setInputCol("vector")
      .setOutputCol("yearvector")
      .setIndices(Array(0))
    // lSlicer.transform(arr2Vect).show(5)

    //Step5: convert type of the label from vector to double (use our Vector2Double)
    val v2d = new Vector2DoubleUDF(year => year.apply(0))
      .setInputCol("yearvector")
      .setOutputCol("rawyear")
    // v2d.transform(lSlicer).show(5)


    //Step6: shift all labels by the value of minimum label such that the value of the smallest becomes 0 (use our DoubleUDF)

    // v2d.select(min(v2d.columns.apply(4))).show()
    // Min year also has been determined in task1
    val lShifter = new DoubleUDF(vector => vector - 1922.0)
      .setInputCol("rawyear")
      .setOutputCol("year")
    // lShifter.transform(v2d).show(5)

    //Step7: extract just the 3 first features in a new vector column
    val fSlicer = new VectorSlicer()
      .setInputCol("vector")
      .setOutputCol("features")
      .setIndices(Array(1,2,3))
    // fSlicer.transform(lShifter).show(5)

    //Step8: put everything together in a pipeline
    val pipeline = new Pipeline()
      .setStages(Array(regexTokenizer, arr2Vect, lSlicer, v2d, lShifter, fSlicer))

    //Step9: generate model by fitting the rawDf into the pipeline
    val pipelineModel = pipeline.fit(songsDf)

    //Step10: transform data with the model - do predictions
    val transformed = pipelineModel.transform(songsDf)

    //Step11: drop all columns from the dataframe other than label and features
    val result = transformed.select("year", "features")
    result.show(5)
  }
}