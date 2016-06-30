package univ.bigdata.course.util

import scala.util.Try

object OptionUtils {
  // Gets the value at array[index] if the index is in the array's bounds
  def maybeIndex[T](array: Array[T], index: Int): Option[T] = {
    if (index < 0 || index >= array.length)
      None
    else
      Some(array(index))
  }

  // Try converting to Int
  def maybeConvertToInt[T](str: String): Option[Int] = {
    Try(Integer.parseInt(str)).toOption
  }

}
