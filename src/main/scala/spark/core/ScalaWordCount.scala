package spark.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by yachao on 17/10/10.
  */
object ScalaWordCount {
  def main(args: Array[String]): Unit = {
    if(args.length != 3){
      println("Usage : java -jar code.jar dependency_jars file_location save_location")
      System.exit(2)
    }

    val jars = ListBuffer[String]()
    args(0).split(',').map(jars += _)

    val conf = new SparkConf()
    conf.setMaster("spark://master:8888")
        .setSparkHome("/usr/local/share/applications/spark-2.1.0-hadoop2.7-bin/bin")
        .setAppName("WordCount")
        .setJars(jars)
        .set("spark.executor.memory", "25g")

    val sc = new SparkContext(conf)

    val rdd = sc.textFile("src/main/resources/data/textfile")
    println("rdd")
    val rdd2 = rdd.flatMap(x => x.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)

    rdd2.filter(_._1.length > 0).map(x => (x._2, x._1)).sortByKey(false).take(50).foreach(println)

    val input = sc.parallelize(
      List(("coffee", 1),
        ("coffee", 3),
        ("panda", 4),
        ("coffee", 5),
        ("street", 2),
        ("panda", 5)))

    input.groupByKey().map(x => (x._1, x._2.sum.toDouble/x._2.size)).foreach(println)
  }
}
