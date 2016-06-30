package univ.bigdata.course.part3

import org.apache.spark.rdd.RDD
import univ.bigdata.course.part3.ExecuteMap.Rank

import scalaz.syntax.std.boolean._

object Map {
  // Optionally get precision value from ranking
  def averagePrecision (ranks : Array[Rank]) : Option[Double] = {
    if (ranks.isEmpty) {
      None // Can happen when intersection of ranked user movies in test set with movies in train set is empty.
    } else {
      val percisionVector = ranks.zipWithIndex.map {
        case (rank, index) => (index + 1).toDouble / (rank + 1).toDouble // ranks start with 0
      }
      Some(percisionVector.sum / percisionVector.length.toDouble)   // Average of precisions
    }
  }

  // Calculate Mean-Average-Percision for rankings
  def calcMap(ranks : Iterator[Array[Rank]]) : Double = {
    val averagePrecisions = ranks.flatMap(averagePrecision).toSeq // Get all precision values
    if (averagePrecisions.isEmpty)
      0.0
    else
      averagePrecisions.sum / averagePrecisions.length  // Mean of precisions
  }
}