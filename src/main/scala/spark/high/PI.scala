package spark.high

import math.random
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yachao on 17/10/21.
  */
object PI {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Spark PI")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val slices = if (args != null && args.length > 0) args(0).toInt else 2
    val n = 100000 * slices

    val count = sc.parallelize(1 to n, slices).map {i =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x * x + y * y < 1) 1 else 0
      }.reduce(_ + _)

    println("PI is roughly : " + (4.0 * count / n))
    sc.stop()
  }
}
