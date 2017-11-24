package spark.simples

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 数据块损坏检索。例如，每个数据块有两个副本，有一个数据块损坏，需要找到那个数据块。
  */
object CountOnce {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Median").setMaster("local[*]")

    val BASE_PATH = "src/main/resources/data"
    val sc = new SparkContext(conf)

    val data = sc.textFile(BASE_PATH + "/countonce.txt")
    val word = data.map(_.toInt)
    val result = word.reduce(_^_)

    println(result)

    sc.stop()
  }
}
