package spark.examples.sort

class SecondSortKey(val firstKey: Int, val secondKey: Int) extends Ordered[SecondSortKey] with Serializable {

  override def compare(that: SecondSortKey): Int = {
    if (this.firstKey != that.firstKey) {
      that.firstKey - this.firstKey
    } else {
      this.secondKey - that.secondKey
    }
  }

  override def toString: String = {
    this.firstKey + "\t" + this.secondKey
  }
}
