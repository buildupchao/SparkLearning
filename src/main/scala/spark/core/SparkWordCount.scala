package spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yachao on 17/10/15.
  */
object SparkWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("SparkWordCount")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("src/main/resources/data/words.txt", 1)
    val results = lines.flatMap(line => line.split("\\s+")).map(x => (x, 1)).reduceByKey(_ + _);

    results.foreach(println)

    println("\n")
    val input = sc.parallelize(
      List(
      ("coffee", 1) ,
      ("coffee", 3) ,
      ("panda",4),
      ("coffee", 5),
      ("street", 2),
      ("panda", 5)))
    input.groupByKey().map(x => (x._1, x._2.sum.toDouble/x._2.size)).foreach(println)

    sc.stop()
  }
}
