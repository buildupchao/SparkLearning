package spark.examples.simples

import org.apache.spark.{SparkConf, SparkContext}

/**
  * WordCount的模型可以在很多场景中使用，如统计过去一年中访客的浏览量、最近一段时间相同查询的数量和海量文本中的词频等。
  */
object SparkWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val BASE_PATH = "src/main/resources/data"
    val sc = new SparkContext(conf)

    val textRDD = sc.textFile(BASE_PATH + "/words.txt")

    val result = textRDD.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.foreach(println)

    sc.stop
  }
}
