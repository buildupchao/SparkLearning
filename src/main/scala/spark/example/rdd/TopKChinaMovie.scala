package spark.example.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yachao on 17/10/29.
  */
object TopKChinaMovie {
  def main(args: Array[String]): Unit = {
    val datapath = "src/main/resources/data/example"
    val conf = new SparkConf()
    conf.setAppName("DifferentCountry")
    conf.setMaster("local[*]")

    val DATA_PATH = datapath
    val sc = new SparkContext(conf)

    val rdd = sc.textFile(DATA_PATH + "/movie_metadata.csv").filter(!_.startsWith("color,director_name"))
    val movieRdd = rdd.map(_.split(","))

    // RDD[(movie_title, director_name, num_voted_users, country, title_year)]
    movieRdd.map(x => (x(11), x(1), x(12), x(20), x(23)))
      .filter(x => x._4.equals("China"))
      .map(x => (x._3.toInt, (x._1, x._2, x._5)))
      .sortByKey(false)
      .map(x => (x._2._1, x._2._2, x._2._3))
      .take(3)
      .foreach(println)
  }
}
