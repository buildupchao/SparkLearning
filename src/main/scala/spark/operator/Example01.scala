package spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yachao on 17/10/21.
  */
object Example01 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Example01")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val list = Seq(Seq(1), Seq(2, 3), Seq(4), Seq(5, 6), Seq(7), Seq(8))
    val rdd = sc.makeRDD(list, 3);

    rdd.flatMap(x => x).foreach(println)

    val rdd2 = sc.makeRDD(1 to 7, 3)
    println("Reduce result: " + rdd2.reduce(_ + _))
  }
}
