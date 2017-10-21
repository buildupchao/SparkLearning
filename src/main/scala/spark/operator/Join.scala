package spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yachao on 17/10/21.
  */
object Join {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Example01")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val pairs1 = Seq(('A', 1), ('B', 1), ('C', 1), ('D', 1), ('A', 2), ('C', 3))
    val rdd1 = sc.makeRDD(pairs1, 3)
    val pairs2 = Seq(('A', 4), ('D', 1), ('C', 1), ('E', 1))
    val rdd2 = sc.makeRDD(pairs2, 2)
    val rdd3 = rdd1.join(rdd2)

    rdd3.foreach(println)
  }
}
