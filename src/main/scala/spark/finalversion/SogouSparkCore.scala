package spark.finalversion

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yachao on 17/11/19.
  */
object SogouSparkCore {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Sougou").setMaster("local")

    val BASE_PATH = "src/main/resources/data"
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(BASE_PATH + "/sogou/SogouQ.sample").map(_.split("\t")).map(x => (x(0).take(5), x(1)))

    val result = rdd.groupByKey().sortByKey(true).map(x => {
      (x._1, x._2.toArray.size, x._2.toSet.size)
    })

    result.foreach(println)
  }
}
