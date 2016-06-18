package univ.bigdata.course.part4

import java.io.{FileOutputStream, PrintStream}
import java.nio.file.{Files, Paths}
import org.apache.spark.rdd.RDD
import univ.bigdata.course.part1.movie.{MovieReview, Movie}
import univ.bigdata.course.part1.preprocessing.MovieIO
import org.apache.spark.graphx._
import univ.bigdata.course.part2.Recommendation._


class pageRank {
  def buildGraph(inputFile: String): Graph[String, Nothing] = {
    val reviews: RDD[MovieReview] = MovieIO.getMovieReviews(inputFile).cache
    val userIDs: RDD[String] = reviews.map(_.userId).distinct(8).cache()
    // get for each movie all the users whom rate it: (MovieName, UsersWhoRateMovieName)
    val usersByMovie: RDD[(String, Iterable[String])] =
      reviews.groupBy(_.movieId).map { case (movie, r) => (movie, r.map(_.userId)) }
    // get rid of the movies themselves, they are an unnecessary information
    val usersByMovieReview: RDD[(Iterable[String])] = usersByMovie.map(_._2)

    // Build RDD for Vertices
    val usersVertex: RDD[(VertexId, String)] = userIDs.map(user => (toID(user), user))
    // Build RDD for Relationships
    val allRelationship: RDD[Iterable[Edge[Nothing]]] =
      usersByMovieReview.map {
        users => users.flatMap(user1 => users.map(user2 => Edge(toID(user1), toID(user2))))
      }
    val relationship: RDD[Edge[Nothing]] = allRelationship.flatMap(edges => edges.map(edge => edge)).distinct().filter(x => x.srcId != x.dstId)

    // Build an return the graph
    Graph(usersVertex, relationship)

  }


      def getPageRank(inputFile : String) = {
        val topK = 100
        val graph = buildGraph(inputFile)
        val ranks: VertexRDD[Double] = graph.pageRank(0.0001).vertices
        // sort by rank and then by id lexicographical order
        val ordering : Ordering[(VertexId, Double)] = Ordering.by(vertex => (-vertex._2,vertex._1))
        // get top 100 users after sorting
        val topUsers: Array[(VertexId, Double)] = ranks.takeOrdered(topK)(ordering)
        // print data in the format: User Id: userId, Rank: UserRank
        topUsers.foreach{
          user => println("User Id: " + user._1 + ", Rank: " + user._2)
        }
        println()

  }

}