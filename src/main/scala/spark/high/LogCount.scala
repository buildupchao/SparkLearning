package spark.high

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yachao on 17/10/21.
  */
object LogCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("LogCount")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val lines = sc.textFile("src/main/resources/data/access.log")

  }
}
