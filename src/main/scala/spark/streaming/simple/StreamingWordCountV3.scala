package spark.streaming.simple

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, StreamingContext}

/**
  * Created by yachao on 17/10/28.
  */
object StreamingWordCountV3 {
  def main(args: Array[String]): Unit = {
    val datapath = "src/main/resources/data/streaming/input"
    val sparkConf = new SparkConf().setAppName("StreamingWordCountV3").setMaster("local[*]")

    // Create the context
    val ssc = new StreamingContext(sparkConf, Minutes(1))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    println("input:" + datapath)

    val lines = ssc.textFileStream(datapath)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    wordCounts.print(10)
    wordCounts.repartition(1).saveAsTextFiles(datapath + "/output/res")

    ssc.start()
    ssc.awaitTermination()
  }
}
