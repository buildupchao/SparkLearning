package spark.high.movie

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet
/**
  * Created by yachao on 17/10/22.
  */
object PopularMovieAnalyzer {
  def main(args: Array[String]): Unit = {
    var datapath = "src/main/resources/data/movie2"
    val conf = new SparkConf().setAppName("PopularMovieAnalyzer")

    if (args.length > 0) {
      datapath = args(0)
    } else {
      conf.setMaster("local[1]")
    }

    val DATA_PATH = datapath
    val sc = new SparkContext(conf)

    /**
      * Step 1: Create RDDs
      */
    val usersRDD = sc.textFile(DATA_PATH + "/users.dat")
    val moviesRDD = sc.textFile(DATA_PATH + "/movies.dat")
    val ratingsRDD = sc.textFile(DATA_PATH + "/ratings.dat")

    val USER_AGE_MIN = "18"
    val USER_AGE_MAX = "24"

    /**
      * Step 2: Extract column from RDDs
      */
    val users = usersRDD.map(_.split("::")).map(x => (x(0), x(2))).filter { x =>
      x._2.compareTo(USER_AGE_MIN) >= 0 && x._2.compareTo(USER_AGE_MAX) <= 0
    }
    val userList = users.map(_._1).collect()

    val userset = HashSet() ++ userList
    val broadcastUserSet = sc.broadcast(userset)

    /**
      * Step 3: map-side join RDD
      */
    val topKmovies = ratingsRDD.map(_.split("::"))
      .map(x => (x(0), x(1)))
      .filter(x => broadcastUserSet.value.contains(x._1))
      .map(x => (x._2, 1))
      .reduceByKey(_ + _)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))
      .take(10)

    /**
      * Step 4: Transform filmID to filmName
      */
    val movieID2Name = moviesRDD.map(_.split("::")).map(x => (x(0), x(1))).collect().toMap

    topKmovies.map(x => (movieID2Name.getOrElse(x._1, null), x._2)).foreach(println)

    sc.stop()
  }
}
