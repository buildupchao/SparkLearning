package spark.high.movie

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yachao on 17/10/27
  */
object TopKMovie {
  def main(args: Array[String]): Unit = {
    val datapath = "src/main/resources/data/movie2"

    val conf = new SparkConf()
    conf.setAppName("TopKMovie")
    conf.setMaster("local")

    val DATA_PATH = datapath
    val sc = new SparkContext(conf)

    /**
      * Step 1: Create RDDs
      */
    val moviesRDD = sc.textFile(DATA_PATH + "/movies.dat")
    val ratingsRDD = sc.textFile(DATA_PATH + "/ratings.dat")

    /**
      * Step 2: Extract columns from RDDs and take top 10
      */
    val movieRDD = ratingsRDD.flatMap(_.split("::"))
      .map(x => (x(1), x(2).toInt))
      .reduceByKey(_ + _)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))
      .take(10)

    /**
      * Step 3: transform filmID to filmName
      */
    val movieID2Name = moviesRDD.flatMap(_.split("::"))
      .map(x => (x(0), x(1)))
      .collect()
      .toMap

    movieRDD.map(x => (movieID2Name.getOrElse(x._1, null), x._2)).foreach(println)

  }
}
