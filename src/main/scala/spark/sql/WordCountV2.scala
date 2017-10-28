package spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by yachao on 17/10/22.
  */
class WordCountV2 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "jangz")
    var datapath = "src/main/resources/data/textfile"
    val conf = new SparkConf().setAppName("DsWordCount")

    if (args != null && args.length > 0) {
      if (args.length == 1) {
        datapath = args(0)
      } else if (args.length == 2) {
        datapath = args(0)
        conf.setMaster(args(1))
      }
    } else {
      conf.setMaster("local[1]")
    }

    val DATA_PATH = datapath
    val spark = SparkSession.builder().config(conf).getOrCreate()

 /*   val result = spark.read.textFile(DATA_PATH).flatMap(_.split("\\s+"))
      .groupByKey(x => x)
      .*/
  }
}
