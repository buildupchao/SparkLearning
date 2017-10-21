package spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yachao on 17/10/21.
  */
object Example02 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("Example02")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val rdd1 = sc.makeRDD(1 to 6, 3)
    val rdd2 = sc.makeRDD(7 to 11, 2)
    val rdd3 = sc.makeRDD(12 to 50, 1)

    val rdd4 = rdd1.union(rdd2)
    rdd4.glom().foreach(println)

    val rdd5 = rdd4.coalesce(3)
    val rdd6 = rdd4.repartition(10)

  }
}
