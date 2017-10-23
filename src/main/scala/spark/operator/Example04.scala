package spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yachao on 17/10/21.
  */
object Example04 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Example04")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val rdd = sc.textFile("src/main/resources/data/input.txt")
      .map(_.split(","))
      .map(x => (x(0), (x(1), x(2), x(3), 1)))

    val result = rdd.mapValues(x => (x._1.toInt, x._2.toInt, x._3.toInt, x._4.toInt))
      .reduceByKey((x, y) => (x._1 + y._1, math.max(x._2, y._2), math.min(x._3, y._3), x._4 + y._4))
      .map { case(x, y) => (x, y._1, y._2, y._3, (y._1.toDouble / y._4))}.collect

    result.foreach(println)
  }
}
