package spark.example.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yachao on 17/10/29.
  */
object CountChinaMovie {
  def main(args: Array[String]): Unit = {
    val datapath = "src/main/resources/data/example"
    val conf = new SparkConf()
    conf.setAppName("DifferentCountry")
    conf.setMaster("local[*]")

    val DATA_PATH = datapath
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df = spark.read.option("header", "true").csv(DATA_PATH + "/movie_metadata.csv")
    
  }
}
