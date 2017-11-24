package spark.examples.simples

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计海量数据时，经常需要预估中位数，由中位数大致了解某列数据，做机器学习和数据挖掘的很多公式中也需要用到中位数。
  */
object Median {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Median").setMaster("local[*]")

    val BASE_PATH = "src/main/resources/data"
    val sc = new SparkContext(conf)

    val l = 1 until 1002

    val rdd = sc.parallelize(l.zip(l))

    val sorted = rdd.sortByKey().map(x => (x._2, x._1))

    val count = sorted.count().toInt

    val median = if (count % 2 == 0) {
      val l = count / 2 - 1
      val r = l + 1

      (sorted.lookup(l).head + sorted.lookup(r).head).toDouble / 2
    } else {
      sorted.lookup(count / 2).head.toDouble
    }

    println("median is : " + median)

    sc.stop
  }
}
