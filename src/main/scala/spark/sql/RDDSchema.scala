package spark.sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by yachao on 17/10/22.
  */
object RDDSchema {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "jangz")
    var datapath = "src/main/resources/data/movie2"
    val conf = new SparkConf().setAppName("RDDSchema")
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

    val schemaString = "userID gender age occupation zipcode"
    val schema = StructType(schemaString.split("\\S+").map(fieldName => StructField(fieldName, StringType, true)))

    val DATA_PATH = datapath
    val usersRDD = sc.textFile(DATA_PATH + "/users.dat")
    val usersRDD2 = usersRDD.map(_.split("::")).map(p => (p(0).toLong, p(1).trim, p(2).toInt, p(3).trim, p(4).trim))

    val userDF = spark.createDataFrame(usersRDD2)
    userDF.take(2).foreach(println)
    println(userDF.count())

//    userDF.write.mode(SaveMode.Overwrite).json("hdfs://master:9000/spark/sql/users.json")
//    userDF.write.mode(SaveMode.Overwrite).parquet("hdfs://master:9000/spark/sql/users.parquet")
    val userJson = spark.read.json("hdfs://master:9000/spark/sql/users.json")
    val userDF2 = userJson.toDF()
    userDF2.take(3).foreach(println)
  }
}
