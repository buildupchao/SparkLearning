package examples

import scala.collection.mutable.ArrayBuffer

/**
  * Created by yachao on 17/10/15.
  */
object Example01 {

  def concat(str1 : String, str2: String) = {
    val strBuffer = new ArrayBuffer[String]()
    for (ch <- str1 if str2.indexOf(ch) != -1) strBuffer += ch.toString()
    strBuffer.mkString(",")
  }

  def main(args: Array[String]): Unit = {
    val str1 = "abcDXYZ"
    val str2 = "Zychaowill"

    println(concat(str1, str2))
  }
}
