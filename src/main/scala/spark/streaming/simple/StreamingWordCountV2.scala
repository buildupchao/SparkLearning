package spark.streaming.simple

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * U can view result by command "nc -lk 9999"
  */
object StreamingWordCountV2 {

  def main(args: Array[String]): Unit = {
    val datapath = "src/main/resources/data/streaming/input"
    val conf = new SparkConf()
    conf.setAppName("StreamingWordCountV2")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    val lines = ssc.textFileStream(datapath)
//    val lines = ssc.socketTextStream("master", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map((_, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()

    // Step 2
    //    wordCounts.saveAsTextFiles("/streaming/output")

    // Step 3
    lines.transform(rdd => {
      rdd.flatMap(_.split(" "))
        .map((_, 1))
        .reduceByKey(_ + _)
    })

    // Step 4
    lines.foreachRDD(rdd => {
      rdd.flatMap(_.split(" "))
        .map((_, 1))
        .reduceByKey(_ + _)
        .take(10)
        .foreach(println)
    })

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the compatation to terminate
  }
}
