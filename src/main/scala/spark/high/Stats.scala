package spark.high

/**
  * Created by yachao on 17/10/21.
  */
class Stats(val count: Int, val numBytes: Int) extends Serializable {
  def merge(other: Stats) = new Stats(count + other.count, numBytes + other.numBytes)

  override def toString = "bytes=%s\tn=%s".format(numBytes, count)
}