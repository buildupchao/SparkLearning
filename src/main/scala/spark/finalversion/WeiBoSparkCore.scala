package spark.finalversion

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yachao on 17/11/19.
  */
object WeiBoSparkCore {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Sougou").setMaster("local")

    val BASE_PATH = "src/main/resources/data"
    val sc = new SparkContext(conf)

    val rdd = sc.textFile(BASE_PATH + "/microblogPCU/follower_followee.csv")

    // (follower_id, followee_id)
    val kvRdd = rdd.map(_.split(",")).map(x => (x(2), x(4)))

    val resultRdd = kvRdd.groupByKey()

    val formattedRdd = resultRdd.map(x => {
      (x._1 + "\t" + x._2.mkString(","))
    })

    formattedRdd.foreach(println)
    formattedRdd.saveAsTextFile(BASE_PATH + "/microblogPCU/result/followee.txt")
  }
}
