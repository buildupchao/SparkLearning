/*
package spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by yachao on 17/10/22.
  */
object CountDifferentAge {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "jangz")
    var datapath = "src/main/resources/data/movie2"
    val conf = new SparkConf().setAppName("CountDifferentAge")
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

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    var one22 = sc.longAccumulator("one22")
    var two30 = sc.longAccumulator("two30")
    var three45 = sc.longAccumulator("three45")
    var four60 = sc.longAccumulator("four60")

    val DATA_PATH = datapath
    val userRDD = spark.sparkContext.textFile(DATA_PATH + "/users.dat").map(_.split("::")).map(x => x(2).toInt)
    userRDD.foreach(x => {
      if (x >= 1 && x < 22) {
        one22.add(1)
      } else if (x >= 22 && x < 30) {
        two30.add(1)
      } else if (x >= 30 && x < 45) {
        three45.add(1)
      } else if (x >= 45 && x < 60) {
        four60.add(1)
      }
    })
    println("[1, 22) => " + one22.value)
    println("[22, 30) => " + two30.value)
    println("[30, 45) => " + three45.value)
    println("[45, 60) => " + four60.value)

    val userDF = spark.read.parquet("hdfs://master:9000/spark/sql/user.parquet")
//    userDF.map(_.getInt(2)).foreach({ x =>
//      if (x >= 1 && x < 22) {
//        "1-22"
//      } else if (x >= 22 && x < 30) {
//        "22-30"
//      } else if (x >= 30 && x < 45) {
//        "30-45"
//      } else if (x >= 45 && x < 60) {
//        "45-60"
//      }
//    })
  }
}
*/
