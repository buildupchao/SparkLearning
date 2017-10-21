package spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yachao on 17/10/21.
  */
object Pair {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Example01")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val pairs = Seq(('A', 1), ('B', 2), ('A', 2), ('C', 4), ('B', 1), ('B', 1), ('D', 1))
    val rdd = sc.makeRDD(pairs, 3)

    //    rdd.mapValues(_ + 1).foreach(println)
    //    rdd.groupByKey().foreach(println)
    val rdd2 = (Seq(('A', 4), ('D', 1), ('E', 1)))

    val s1 = Seq('A', 'B', 'C', 'D', 'E')
    val rdd3 = sc.makeRDD(s1)
    val rdd4 = rdd3.zipWithIndex()
    rdd4.foreach(println)
  }
}
