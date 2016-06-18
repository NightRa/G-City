package univ.bigdata.course.part4

import java.io.{FileOutputStream, PrintStream}
import java.nio.file.{Files, Paths}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import univ.bigdata.course.SparkMain
import univ.bigdata.course.part1.CommandsTask
import univ.bigdata.course.part1.movie.{MovieReview, Movie}
import univ.bigdata.course.part1.preprocessing.MovieIO
import org.apache.spark.graphx._
import univ.bigdata.course.part2.Recommendation._
import org.apache.spark.graphx.PartitionStrategy._
import org.apache.spark.graphx.lib._

/**
  * Created by Charlie on 18-Jun-16.
  */
class pageRank {
  def buildGraph(inputFile: String): Graph[String, Nothing] = {
    val reviews: RDD[MovieReview] = MovieIO.getMovieReviews(inputFile)
    val userIDs: RDD[String] = reviews.map(_.userId).distinct(8).cache()
    val usersByMovie: RDD[(String, Iterable[String])] = reviews.groupBy(_.movieId).map { case (movie, r) => (movie, r.map(_.userId)) }
    val usersByMovieReview: RDD[(Iterable[String])] = usersByMovie.map(_._2)

    //Vertices
    val usersVertex: RDD[(VertexId, String)] = userIDs.map(user => (toID(user), user))
    //Relationships
    val allRelationship: RDD[Iterable[Edge[Nothing]]] =
      usersByMovieReview.map {
        users => users.flatMap(user1 => users.map(user2 => Edge(toID(user1), toID(user2))))
      }
    val relationship: RDD[Edge[Nothing]] = allRelationship.flatMap(edges => edges.map(edge => edge)).distinct().filter(x => x.srcId != x.dstId)

    Graph(usersVertex, relationship)
  }


  //    def pagerank(inputFile : String) = {
  //      val graph = buildGraph(inputFile)
  //      val ranks = graph.pageRank(0.0001).vertices
  //}

}