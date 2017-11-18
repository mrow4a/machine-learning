package se.kth.spark.lab1.task6

import org.apache.spark._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{PolynomialExpansion, RegexTokenizer, VectorAssembler, VectorSlicer}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import se.kth.spark.lab1.{Array2Vector, DoubleUDF, Vector2DoubleUDF}

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val filePath = "src/main/resources/million-song.txt"
    val songsDf = spark.sparkContext.textFile(filePath)
      // Remove all " chars
      .map(record => record.replace("\"", ""))
      .toDF("record")
      .cache()
    val numSongs = songsDf.count()
    println(s"Songs: $numSongs")

    // Pre-process data (pipeline)
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("record")
      .setOutputCol("record_array")
      .setPattern(",")
    val arr2Vect = new Array2Vector()
      .setInputCol("record_array")
      .setOutputCol("record_vector")

    // Prepare label (pipeline)
    val lSlicer = new VectorSlicer()
      .setInputCol("record_vector")
      .setOutputCol("year_vector")
      .setIndices(Array(0))
    val v2d = new Vector2DoubleUDF(year => year.apply(0))
      .setInputCol("year_vector")
      .setOutputCol("rawyear")
    val lShifter = new DoubleUDF(vector => vector - 1922.0)
      .setInputCol("rawyear")
      .setOutputCol("label")

    // Prepare features (pipeline)
    val fSlicer = new VectorSlicer()
      .setInputCol("record_vector")
      .setOutputCol("features")
      .setIndices(Array(1,2,3))

    // prepare linear regression
    val lrStage = new MyLinearRegressionImpl()
      .setLabelCol("label")
      .setFeaturesCol("features")

    // Construct linear regression training pipeline
    val lrPipeline = new Pipeline()
      .setStages(Array(
        regexTokenizer,
        arr2Vect,
        lSlicer,
        v2d,
        lShifter,
        fSlicer,
        lrStage))

    // Train Linear Regression Model on training data
    val lrModel: PipelineModel = lrPipeline.fit(songsDf)

    // Get predictions for test data
    lrModel.transform(songsDf)
      .select("label", "features", "prediction")
      .show(5)

    //print rmse of our model
    val trainingError = lrModel
      .stages(6)
      .asInstanceOf[MyLinearModelImpl].trainingError
    println(s"RMSE: ${trainingError.apply(trainingError.length-1)} " +
      s"for custom linear regression ")
  }
}