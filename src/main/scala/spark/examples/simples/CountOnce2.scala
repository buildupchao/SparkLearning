package spark.examples.simples

import org.apache.spark.{SparkConf, SparkContext}

object CountOnce2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CountOnce2").setMaster("local[*]")

    val BASE_PATH = "src/main/resources/data"
    val sc = new SparkContext(conf)

    val data = sc.textFile(BASE_PATH + "/countonce.txt")

    val result = data.mapPartitions(iter => {
      var temp = iter.next().toInt
      while (iter.hasNext) {
        temp = temp^iter.next().toInt
      }
      Seq((1, temp)).iterator
    }).reduceByKey(_^_).collect()

    println("num appear once is :" + result(0))

    sc.stop()
  }
}
