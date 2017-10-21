package spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yachao on 17/10/21.
  */
object Example03 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Example03")
    conf.setMaster("local")

    val sc = new SparkContext(conf)

    val input = sc.parallelize(List(("coffee", 1), ("coffee", 3),
      ("panda", 4), ("coffee", 5), ("street", 2), ("panda", 5)))

    val rdd = input.map(x => (x._1, (x._2, 1)))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map { case(x, y) => { (x, y._1.toDouble / y._2) } }
      .collect

    rdd.foreach(println)

    /*val rdd2 = input.groupByKey().mapValues(_.toArray).map(x => (x._1, x._2.sum.toDouble / x._2.length));
    rdd2.foreach(println)*/
  }
}
