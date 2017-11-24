package spark.examples.simples

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Top K的模型可以应用在求过去一段时间消费次数最多的消费者、访问最频繁的IP地址和最近更新最频繁的微博等应用场景。
  */
object TopK {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TopK").setMaster("local[*]")

    val BASE_PATH = "src/main/resources/data"
    val sc = new SparkContext(conf)

    /**
      * Execute WordCount to count the highest frequency words
      */
    val result = sc.textFile(BASE_PATH + "/topk.txt")
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .map(word => (word._2, word._1))
      .sortByKey(false)
      .map(word => (word._2, word._1))
      .take(2)
      .foreach(println)

    sc.stop
  }
}
