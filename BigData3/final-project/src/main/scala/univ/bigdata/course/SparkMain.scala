package univ.bigdata.course

import org.apache.spark.{SparkConf, SparkContext}

object SparkMain {
  lazy val conf = new SparkConf().setAppName("Big Data HW3")
  lazy val sc = new SparkContext(conf)

  def main(args: Array[String]) {
    println("Hello world from Scala!")
    conf
    sc

  }
}
