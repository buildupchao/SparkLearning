package spark.high.movie

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yachao on 17/10/21.
  */
object Task01 {

  val conf = new SparkConf()
  conf.setAppName("LogCount")
  conf.setMaster("local")
  val sc = new SparkContext(conf)
  val BASE_PATH = "src/main/resources/data/movie"
  val usersRDD = sc.textFile(BASE_PATH + "/tags.csv")
  val ratingsRDD = sc.textFile(BASE_PATH + "/ratings.csv")
  val moviesRDD = sc.textFile(BASE_PATH + "/movies.csv")

  def main(args: Array[String]): Unit = {
    val MOVIETITLE = "GoldenEye (1995)"
    userAgeAndGender(MOVIETITLE)
  }

  def userAgeAndGender(MOVIE_TITLE: String): Unit = {
    val users = usersRDD.map(_.split(",")).map(x => (x(0), (x(1), x(2))))
    val ratings = ratingsRDD.map(_.split(",")).map(x => (x(0), x(1)))
    val movies = moviesRDD.map(_.split(",")).map(x => (x(0), x(1)))

    val movie = movies.filter(_._2.equals(MOVIE_TITLE)).take(1)

    if (movie.length <= 0) {
      return
    }
    val MOVIE_ID = movie(0)._1
    val usermovie = ratings.filter(_._2.equals(MOVIE_ID))
    val userRating = users.join(usermovie)

    val result = userRating.map { x =>
      (x._2._2, 1)
    }.reduceByKey(_ + _)

    result.collect.foreach(println)
  }
}
