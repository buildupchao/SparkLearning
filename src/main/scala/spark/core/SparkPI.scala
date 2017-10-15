package spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yachao on 17/10/15.
  */
object SparkPI {

  import math.random

  def main(args: Array[String]): Unit = {

    val n = 1000000

    val conf = new SparkConf().setAppName("SparkPI").setMaster("local");
    val count = new SparkContext(conf).parallelize(0 until n, 100).map(i => {
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y < 1) 1 else 0
    }).reduce(_ + _)
    println("PI is roughly " + 4.0 * count / n)
  }
}
