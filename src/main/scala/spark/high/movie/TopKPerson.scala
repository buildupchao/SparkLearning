package spark.high.movie

import org.apache.spark.{SparkConf, SparkContext}

object TopKPerson {
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
    val usersRDD = sc.textFile(DATA_PATH + "/users.dat")
    val ratingsRDD = sc.textFile(DATA_PATH + "/ratings.dat")

    /**
      * Step 2: Extract columns from RDDs and take top 10
      */
    val ratingRDD = ratingsRDD.flatMap(_.split("::"))
      .map(x => (x(0), 1))
      .reduceByKey(_ + _)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))
      .take(10)

    val userRDD = usersRDD.flatMap(_.split("::"))
      .map(x => (x(0), (x(1), x(2), x(3), x(4))))
      .collect()
      .toMap

    /**
      * Step 3: result
      */
    ratingRDD.map(x => (x._1, userRDD.getOrElse(x._1, null))).foreach(println)
  }
}
