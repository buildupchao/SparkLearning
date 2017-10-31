package spark.example.king

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by yachao on 17/10/29.
  */
object TopKPerson {
  def main(args: Array[String]): Unit = {
    val datapath = "src/main/resources/data/example"
    val conf = new SparkConf()
    conf.setAppName("DifferentCountry")
    conf.setMaster("local[*]")

    val DATA_PATH = datapath
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val df = spark.read.option("header", "true").csv(DATA_PATH + "/EmailReceivers.csv")
    df.createOrReplaceTempView("receivers")

    spark.sql("select PersonId, count(Id) as c from receivers group by personId, ")
  }
}
