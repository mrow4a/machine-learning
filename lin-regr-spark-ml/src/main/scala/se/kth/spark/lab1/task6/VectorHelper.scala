package se.kth.spark.lab1.task6

import org.apache.spark.ml.linalg.{Matrices, Vector, Vectors}

object VectorHelper {
  def dot(v1: Vector, v2: Vector): Double = {
    var sum = 0.0
    val range = v1.size - 1
    for( i <- 0 to range){
      sum += (v1.apply(i) * v2.apply(i))
    }

    sum
  }

  def dot(v: Vector, s: Double): Vector = {
    var vector = new Array[Double](v.size)
    for( i <- 0 until v.size){
      vector.update(i, v.apply(i) * s)
    }

    Vectors.dense(vector)
  }

  def sum(v1: Vector, v2: Vector): Vector = {
    var vector = new Array[Double](v1.size)
    for( i <- 0 until v1.size) {
      vector.update(i, v1.apply(i) + v2.apply(i))
    }

    Vectors.dense(vector)
  }

  def fill(size: Int, fillVal: Double): Vector = {
    var vector = new Array[Double](size)
    for( i <- 0 until size){
      vector.update(i, fillVal)
    }

    Vectors.dense(vector)
  }
}