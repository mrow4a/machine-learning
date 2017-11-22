package se.kth.spark.lab1.task1

import se.kth.spark.lab1._

import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.Pipeline
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

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
    val rdd = spark.sparkContext.textFile(filePath)

    //Step1: print the first 5 rows, what is the delimiter, number of features and the data types?
    for (sample <- rdd.take(5)) {
      println(sample)
    }

    //Step2: split each row into an array of features
    val recordsRdd = rdd.map(s => s.split(',').toVector)

    //Step3: map each row into a Song object by using the year label and the first three features
    val songsRdd = recordsRdd.map(record => Obs(
      record.apply(0).toDouble,
      record.apply(1).toDouble,
      record.apply(2).toDouble,
      record.apply(3).toDouble)
    )

    //Step4: convert your rdd into a datafram
    val songsDf = songsRdd.toDF().cache()
    songsDf.createOrReplaceTempView("songs")

    //Step5: Do some analysis
    songsDf.show(5)
    spark.sql("SELECT COUNT(*) FROM songs").show()
    spark.sql("SELECT MAX(year), MIN(year), AVG(year) FROM songs").show()
    spark.sql("SELECT COUNT(*) FROM songs WHERE year >= 2000.0 AND year <= 2010.0").show()
    spark.sql("SELECT year, COUNT(year) FROM songs " +
      "WHERE year >= 2000.0 AND year <= 2010.0 " +
      "GROUP BY year "
    ).show()
  }
}