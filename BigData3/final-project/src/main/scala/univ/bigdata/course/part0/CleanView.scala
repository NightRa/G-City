package univ.bigdata.course.part0

import java.nio.file.{Files, Paths}

import org.apache.spark.rdd.RDD
import univ.bigdata.course.SparkMain
import univ.bigdata.course.part1.movie.MovieReview
import univ.bigdata.course.part1.preprocessing.MovieIO

object CleanView extends App {
  SparkMain.init()
  val reviews: RDD[MovieReview] = MovieIO.getMovieReviews("BigDataset/movies-simple4.txt")
  val out = reviews.collect().map(r => r.userId + '\t' + r.movieId + '\t' + r.score.toInt).mkString("\n")
  Files.write(Paths.get("clean-movies-big.txt"), out.getBytes)
}
