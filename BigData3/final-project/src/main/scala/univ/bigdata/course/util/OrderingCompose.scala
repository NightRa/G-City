package univ.bigdata.course.util

object OrderingCompose {

  def compose[A](ord1: Ordering[A], ord2: Ordering[A]): Ordering[A] = new Ordering[A] {
    override def compare(x: A, y: A): Int = {
      val comp1 = ord1.compare(x, y)
      if (comp1 == 0) ord2.compare(x, y)
      else comp1
    }
  }

}
