package spark.finalversion

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by yachao on 17/11/19.
  */
object SogouSparkDF {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Sougou").setMaster("local")

    val BASE_PATH = "src/main/resources/data"
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    val df = spark.read.textFile(BASE_PATH + "/sogou/SogouQ.sample").map(_.split("\t")).map(x => (x(0).take(5), x(1))).toDF("time", "userid")

    df.createGlobalTempView("logView");

//    df.agg("userid" -> "count", "userid" -> "countDistinct").groupBy("time")
    spark.sql("select time, count(userid), count(distinct(userid)) from global_temp.logView group by time order by time").rdd.foreach(println)

  }
}
