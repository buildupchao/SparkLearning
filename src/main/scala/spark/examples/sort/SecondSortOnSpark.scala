package spark.examples.sort

import org.apache.spark.{SparkConf, SparkContext}

object SecondSortOnSpark {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SecondSortOnSpark").setMaster("local[*]")

    val BASE_PATH = "src/main/resources/data/examples"
    val sc = new SparkContext(conf)

    val lines = sc.textFile(BASE_PATH + "/secondsort.txt")

    val pairSortKey = lines.map(_.split(" ")).map(numbers => {
      (new SecondSortKey(numbers(0).toInt, numbers(1).toInt), numbers)
    })

    var sortPair = pairSortKey.sortByKey(false)

    val sortResult = sortPair.map(_._1)

    sortResult.collect().foreach(println)

    sc.stop()
  }
}
