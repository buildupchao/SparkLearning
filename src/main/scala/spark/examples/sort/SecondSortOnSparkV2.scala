package spark.examples.sort

import org.apache.spark.{SparkConf, SparkContext}

object SecondSortOnSparkV2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SecondSortOnSparkV2").setMaster("local[*]")

    val BASE_PATH = "src/main/resources/data/examples"
    val sc = new SparkContext(conf)

    val lines = sc.textFile(BASE_PATH + "/secondsort.txt")

    val pairs = lines.map(_.split(" ")).map(numbers => {
      (numbers(0).toInt, numbers(1).toInt)
    })

    implicit val sorting = new Ordering[(Int, Int)] {
      override def compare(x: (Int, Int), y: (Int, Int)): Int = {
        val equalFirst = x._1.compareTo(y._1)
        if (equalFirst == 0) {
          -x._2.compareTo(y._2)
        } else {
          equalFirst
        }
      }
    }

//    pairs.sortBy(sorting)

//    sortResult.collect().foreach(println)

    sc.stop()
  }
}
