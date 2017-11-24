package spark.examples.topk.groupsort

import org.apache.spark.{SparkConf, SparkContext}

object GroupByAgeDescScoreExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GroupByAgeDescScoreExample").setMaster("local[*]")

    val BASE_PATH = "src/main/resources/data/examples"
    val sc = new SparkContext(conf)

    val data = sc.textFile(BASE_PATH + "/groupAndsort.txt")

    val pairs = data.map(_.split(","))
      .map(document => (document(1).toInt, (document(0), document(1).toInt, document(2), document(3).toInt)))
      .groupByKey()

    // 元组自定义排序
    implicit  val sorting = new Ordering[(String, Int, String, Int)] {
      override def compare(x: (String, Int,String,  Int), y: (String, Int, String, Int)): Int = {
        -x._4.compareTo(y._4)
      }
    }

    val result = pairs.mapValues(x => x.toList.sorted(sorting))

    result.foreach(println)

    sc.stop()
  }
}
