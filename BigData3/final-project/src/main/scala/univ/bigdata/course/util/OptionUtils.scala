package univ.bigdata.course.util

import scala.util.Try

object OptionUtils {
  def maybeIndex[T](array: Array[T], index: Int): Option[T] = {
    if (index < 0 || index >= array.length)
      None
    else
      Some(array(index))
  }

  def maybeConvertToInt[T](str: String): Option[Int] = {
    Try(Integer.parseInt(str)).toOption
  }

  implicit class BoolToOption(val self: Boolean) extends AnyVal {
    def toOption[A](value: => A): Option[A] =
      if (self) Some(value) else None
  }

}
