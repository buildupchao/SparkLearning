package spark.high.movie

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.HashSet

/**
  * Created by yachao on 17/10/21.
  */
object Task02 {

  def main(args: Array[String]): Unit = {
    var dataPath = "src/main/resources/data/movie2"
    val conf = new SparkConf().setAppName("PopularMovieAnalyzer")
    if (args != null && args.length > 0) {
      dataPath = args(0)
    } else {
      conf.setMaster("local[1]")
    }

    val sc = new SparkContext(conf)

    val BASE_PATH = dataPath
    val USER_AGE = "18"

    /**
      * Step 1: Create RDDs
      */
    val usersRDD = sc.textFile(BASE_PATH + "/users.dat")
    val ratingsRDD = sc.textFile(BASE_PATH + "/ratings.dat")
    val moviesRDD = sc.textFile(BASE_PATH + "/movies.dat")

    /**
      * Step 2: Extract columns from RDDS
      */
    val users = usersRDD.map(_.split("::")).map(x => (x(0), x(2))).filter(_._2.equals(USER_AGE))

    // Array[String]
    val userlist = users.map(_._1).collect()

    // broadcast
    val userSet = HashSet() ++ userlist
    val broadcastUserSet = sc.broadcast(userSet)

    /**
      * Step 3: map-join RDDs
      */
    val topMovies = ratingsRDD.map(_.split("::"))
      .map(x => (x(0), x(1)))
      .filter(x => broadcastUserSet.value.contains(x._1))
      .map(x => (x._2, 1))
      .reduceByKey(_ + _)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))
      .take(10)

    /**
      * Transform filmID to filmName
      */
    val movieID2Name = moviesRDD.map(_.split("::"))
      .map(x => (x(0), x(1)))
      .collect()
      .toMap

    topMovies.map(x => (movieID2Name.getOrElse(x._1, null), x._2))
      .foreach(println)

    sc.stop()
  }

}
