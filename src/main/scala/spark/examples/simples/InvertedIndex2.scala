package spark.examples.simples

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 搜索引擎的关键步骤是建立倒排索引
  *
  * 应用场景：搜索引擎及垂直搜索引擎中需要构建倒排索引，文本分析中有的场景也需要构建倒排索引。
  */
object InvertedIndex2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("InvertedIndex2").setMaster("local[*]")

    val BASE_PATH = "src/main/resources/data"
    val sc = new SparkContext(conf)

    val data = sc.textFile(BASE_PATH + "/invertedindex.txt")

    val keydata = data.map(_.split("\t")).map(item => (item(0), item(1)))

    val inverting = keydata.flatMap(file => {
      val list = new mutable.LinkedHashMap[String, String]()
      val words = file._2.split(" ").iterator

      while (words.hasNext) {
        list.put(words.next(), file._1)
      }
      list
    })

    inverting.reduceByKey(_ + " " + _).map(pair => pair._1 + "\t" + pair._2).foreach(println)

    sc.stop()
  }
}
