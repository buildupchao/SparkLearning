package examples

/**
  * Created by yachao on 17/10/15.
  */
object Example02 {

  def showOdd(data: Range) {
    data.filter(_ % 2 != 0).foreach(println)
  }

  def sumRange(data: Range): Int = {
    data.sum
  }

  def main(args: Array[String]): Unit = {
    val data = 5 until 100
//    showOdd(data)
    println(sumRange(data))
  }
}
