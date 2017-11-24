package spark.simples

import org.apache.spark.{SparkConf, SparkContext}

object TopKIp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TopKIp").setMaster("local[*]")

    val BASE_PATH = "src/main/resources/data"
    val sc = new SparkContext(conf)

    /**
      * Execute WordCount to count the highest frequency words
      */
    val data = sc.textFile(BASE_PATH + "/ip.txt")

    val result = data.map(line => """\d+\.\d+\.\d+\.\d+""".r.findAllIn(line).mkString)
      .filter(!_.equals(""))
      .map((_, 1))
      .reduceByKey(_ + _)
      .map(word => (word._2, word._1))
      .sortByKey(false)
      .map(word => (word._2, word._1))
      .take(10)

    result.foreach(println)

    sc.stop
  }
}
