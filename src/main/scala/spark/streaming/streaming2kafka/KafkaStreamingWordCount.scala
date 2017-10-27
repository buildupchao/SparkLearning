package spark.streaming.streaming2kafka

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

/**
  * @author jangz
  * @see http://www.cnblogs.com/hseagle/p/3887507.html
  */
object KafkaStreamingWordCount {

  def main(args: Array[String]): Unit = {
    val zkQuorum = "master:2181"
    val group = "1"
    val topics = "top1,top2"
    val numThreads = 2

    val conf = new SparkConf()
    conf.setAppName("KafkaStreamingWordCount")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(2))
    val topicMap = topics.split(",").map((_, numThreads)).toMap

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)

    wordCounts.print()

    wordCounts.foreachRDD(rdd => {
      rdd.foreachPartition(p => {
        p.foreach(println)
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
