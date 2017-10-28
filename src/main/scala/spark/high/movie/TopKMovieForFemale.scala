/*
package spark.high.movie

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object TopKMovieForFemale {
  def main(args: Array[String]): Unit = {
    val datapath = "src/main/resources/data/movie2"

    val conf = new SparkConf()
    conf.setAppName("TopKMovie")
    conf.setMaster("local")

    val DATA_PATH = datapath
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    // all female
    import spark.implicits._
    val usersDF = spark.read.textFile(DATA_PATH + "/users.dat")
      .flatMap(_.split("::"))
      .map(x => (x(0), x(1)))
      .filter(x => (x._2 == 'F'))
      .map(x => x._1)
      .toDF("UserID")

    import spark.implicits._
    val ratingsDF = spark.read.textFile(DATA_PATH + "/ratings.dat")
      .flatMap(_.split("::"))
      .map(x => (x(0), x(1), x(2)))
      .toDF("UserID", "MovieID", "Rating")

    // DF(UserID, MovieID, Rating)
    val movieRDD = usersDF.join(ratingsDF)
      .map(x => (x(1), 1))
      .rdd
      .reduceByKey(_ + _)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))
      .take(10)
      .toMap

    import spark.implicits._
    val moviesDF = spark.read.textFile(DATA_PATH + "/movies.dat")
      .flatMap(_.split("::"))
      .map(x => (x(0), x(1)))
      .toDF("MovieID", "Title")

    moviesDF.filter(x => movieRDD.contains(x(1))).select("Title").rdd.foreach(println)
  }
}
*/
