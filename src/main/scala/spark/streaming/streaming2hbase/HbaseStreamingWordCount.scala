package spark.streaming.streaming2hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object HbaseStreamingWordCount {

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
      rdd.foreachPartition(partitionOfRecords => {
        val tableName = "hamlet"
        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.quorum", "master:2181")
        val htable = new HTable(hbaseConf, TableName.valueOf(tableName))
        partitionOfRecords.foreach(pair => {
          val put = new Put(Bytes.toBytes(rdd.id))
          put.addColumn("words".getBytes, Bytes.toBytes(pair._1), Bytes.toBytes(pair._2))
          htable.put(put)
        })
        htable.close
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
