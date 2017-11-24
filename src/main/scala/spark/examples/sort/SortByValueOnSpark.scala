package spark.examples.sort

import org.apache.spark.{SparkConf, SparkContext}

object SortByValueOnSpark {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SortByValueOnSpark").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val d1 = sc.parallelize(Array(("cc",12),("bb",32),("cc",22),("aa",18),("bb",16),("dd",16),("ee",54),("cc",1),("ff",13),("gg",68),("bb",4)))
    d1.reduceByKey(_+_).sortBy(_._2,false).collect.foreach(println)

    sc.stop()
  }
}
