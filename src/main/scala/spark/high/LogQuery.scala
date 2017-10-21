package spark.high

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yachao on 17/10/21.
  */
object LogQuery {

  val apacheLogRegex = """^([\d.]+)(\S+)(\S+)\[([\w\d:/]+\s[+\-]\d{4})\]"(.+?)"(\d{3})([\d\-]+)"([^"]+)""([^"]+)".*""".r

  def extractKey(line: String): (String, String, String) = {
    apacheLogRegex.findFirstIn(line) match {
      case Some(apacheLogRegex(ip, _, user, dateTime, query, status, bytes, referer, ua)) => if (user != "\"-\"") (ip, user, query)
      else (null, null, null)
      case _ => (null, null, null)
    }
  }

  def extractStats(line: String): Stats = {
    apacheLogRegex.findFirstIn(line) match {
      case Some(apacheLogRegex(ip, _, user, dateTime, query, status, bytes, referer, ua)) => new Stats(1, bytes.toInt)
      case _ => new Stats(1, 0)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Log Query")
    val sc = new SparkContext(conf)
    val dataset = sc.textFile(args(0))
    dataset.map(line => (extractKey(line), extractStats(line)))
      .reduceByKey((a, b) => a.merge(b))
      .collect().foreach {
      case (user, query) => println("%s\t%s".format(user, query))
    }
  }
}
