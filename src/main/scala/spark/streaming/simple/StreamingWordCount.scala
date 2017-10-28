package spark.streaming.simple

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingWordCount {

  val updateFunc = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map(x => {
      (x._1, x._2.sum + x._3.getOrElse(0))
    })
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("StreamingWordCount")
    conf.setMaster("local")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    // pull information of socket
    val dStream = ssc.socketTextStream("master", 10086)

    // must set setCheckpointDir operator when use updateStateByKey
    sc.setCheckpointDir("")
    // wordcount
    val res = dStream.flatMap(_.split(" ")).map((_, 1)).updateStateByKey(updateFunc, new HashPartitioner(sc.defaultParallelism), true)
    // every batch of wordcount
//    val res2 = dStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    res.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
