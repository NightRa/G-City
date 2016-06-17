package univ.bigdata.course.part3

import org.apache.spark.rdd.RDD
import univ.bigdata.course.part3.ExecuteMap.Rank
import scalaz.syntax.std.boolean._

object Map {
  def averagePrecision (ranks : Vector[Rank]) : Option[Double] = {
    ranks.isEmpty.option { // Can happen when intersection of ranked user movies in test set with movies in train set is empty.
      val percisionVector = ranks.zipWithIndex.map {
        case (rank, index) => (index + 1).toDouble / (rank + 1).toDouble // ranks start with 0
      }
      percisionVector.sum / percisionVector.length.toDouble
    }
  }

  def calcMap(ranks : RDD[Vector[Rank]]) : Double = {
    val averagePrecisions = ranks.flatMap(averagePrecision).cache()
    if (averagePrecisions.isEmpty())
      0.0
    else
      averagePrecisions.mean()
  }
}