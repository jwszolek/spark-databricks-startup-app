package io.datamass

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.math.random

object Runner extends App {

  val goodSparkContext = SparkContext.getOrCreate()
  val goodSparkSession = SparkSession.builder().getOrCreate()

    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = goodSparkContext.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y <= 1) 1 else 0
    }.reduce(_ + _)

    println(s"Pi is roughly ${4.0 * count / (n - 1)}")

  println("test")

}
