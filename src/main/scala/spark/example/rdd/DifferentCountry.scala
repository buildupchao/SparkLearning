package spark.example.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yachao on 17/10/29.
  */
object DifferentCountry {
  def main(args: Array[String]): Unit = {
    val datapath = "src/main/resources/data/example"
    val conf = new SparkConf()
    conf.setAppName("DifferentCountry")
    conf.setMaster("local[*]")

    val DATA_PATH = datapath
    val sc = new SparkContext(conf)

    val rdd = sc.textFile(DATA_PATH + "/movie_metadata.csv").filter(!_.startsWith("color,director_name"))
    val movieRdd = rdd.map(_.split(","))

    movieRdd.filter(_.size == 28).map(x => x(20)).distinct().foreach(println)
  }
}
