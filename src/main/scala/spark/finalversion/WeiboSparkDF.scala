package spark.finalversion

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yachao on 17/11/19.
  */
object WeiboSparkDF {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Sougou").setMaster("local")

    val BASE_PATH = "src/main/resources/data"

    val spark = SparkSession.builder().config(conf).getOrCreate()
    val df = spark.read.option("header", true).csv(BASE_PATH + "/microblogPCU/follower_followee.csv").select("follower_id", "followee_id")

    import org.apache.spark.sql.functions._
    val result = df.groupBy("follower_id").agg(collect_set("followee_id"))


  }
}
