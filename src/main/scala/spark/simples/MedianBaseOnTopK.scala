package spark.simples

import org.apache.spark.{SparkConf, SparkContext}

object MedianBaseOnTopK {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val BASE_PATH = "src/main/resources/data"
    val sc = new SparkContext(conf)

    val l = 1 until 1002

    val data = sc.parallelize(l)

    /**
      * 将数据逻辑划分为10个桶，这里可以自行设置桶数量，统计每个桶中落入的数据量
      */
    val number = data.map(num => (num/4, num)).sortByKey()
    val pariCount = data.map(num => (num/4, num)).reduceByKey(_ + _).sortByKey()


    /**
      * 根据总的数据量，逐次根据桶序号由低到高依次累加，判断中位数落在哪个桶中并获取到中位数在桶中的偏移量
      */
    val count = data.count().toInt
    var mid = 0
    if (count % 2 == 0) {
      mid = count/2 + 1
    } else {
      mid = count/2
    }

    /**
      * 中位数在桶中的偏移量
      */
    var temp = 0
    var temp1 = 0
    var index = 0
    var totalNumber = pariCount.count().toInt

    var fountId = false
    for (i <- 0 to totalNumber - 1 if !fountId) {
      temp = temp + pariCount.collectAsMap()(i)
      temp1 = temp - pariCount.collectAsMap()(i)
      if (temp >= temp1) {
        index = i
        fountId = true
      }
    }
    val offset = mid - temp1

    /**
      *
      */
    val median = number.filter(_._1 == index).takeOrdered(offset)
    sc.setLogLevel("ERROR")
    println("median is : " + median(offset - 1)._2)

    sc.stop
  }
}
