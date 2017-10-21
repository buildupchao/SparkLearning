package spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yachao on 17/10/15.
  */
object IntDataSetOper {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf();
    conf.setAppName("IntDataSetOper")
    conf.setMaster("local")

    val sc = new SparkContext(conf);
    val input = sc.parallelize(100 to 1000);
    print("5 elements: ")
    input.take(5).foreach(x => print(x + "\t"))
    println()

    println("sum: " + input.sum())

    println("average: " + (input.sum() / input.count()))

    println(input.filter(_ % 2 == 0).count())
    input.filter(_ % 2 == 0).take(5).foreach(println)
  }
}
