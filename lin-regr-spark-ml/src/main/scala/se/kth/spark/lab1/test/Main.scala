package se.kth.spark.lab1.test

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import java.net.URL

import org.apache.commons.io.IOUtils

case class Bank(age: Integer, job: String, marital: String, education: String, balance: Integer)

object Main {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("lab1").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    import sqlContext._

    val url = new URL("https://s3.amazonaws.com/apache-zeppelin/tutorial/bank/bank.csv")
    val bankText = sc.parallelize(
      IOUtils.toString(
        url.openStream(),
        "utf8").split("\n"))

    val bank = bankText.map(s => s.split(";")).filter(s => s(0) != "\"age\"").map(
      s => Bank(s(0).toInt,
        s(1).replaceAll("\"", ""),
        s(2).replaceAll("\"", ""),
        s(3).replaceAll("\"", ""),
        s(5).replaceAll("\"", "").toInt)).toDF()
    bank.registerTempTable("bank")
    bank.show(5)
    sqlContext.sql("select age, count(1) from bank where age < 70 group by age order by age").show()
  }
}