package spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yachao on 17/10/15.
  */
object WordCount {
  def main(args: Array[String]) {
    var masterUrl = "local[1]"
    var inputPath = "word/"

    if (args.length == 1) {
      masterUrl = args(0)
    } else if (args.length == 2) {
      masterUrl = args(0)
      inputPath = args(1)
    }

    println(s"masterUrl:${masterUrl}, inputPath: ${inputPath}")

    val sparkConf = new SparkConf().setMaster(masterUrl).setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rowRdd = sc.textFile(inputPath)
    val resultRdd = rowRdd.flatMap(line => line.split("\\s+"))
      .map(word => (word, 1)).reduceByKey(_ + _)

    resultRdd.take(20).foreach(println)
  }
}
