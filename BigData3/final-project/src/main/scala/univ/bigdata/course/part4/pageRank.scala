package univ.bigdata.course.part4

import java.io.{FileOutputStream, PrintStream}
import java.nio.file.{Files, Paths}
import org.apache.spark.rdd.RDD
import univ.bigdata.course.part1.movie.{MovieReview, Movie}
import univ.bigdata.course.part1.preprocessing.MovieIO
import org.apache.spark.graphx._
import univ.bigdata.course.part2.Recommendation._


object PageRank {
  def CreateGraph(inputFilePath: String): Graph[String, String] = {
	// Parse the file to reviews
    val reviews: RDD[MovieReview] = MovieIO.getMovieReviews(inputFilePath).cache()
	
	// Get all users from reviews
    val users: RDD[String] = reviews
      .groupBy(_.userId)
      .map { case (userId, reviews) => userId }.cache()

    // Create vertices of the Graph
    val vertices: RDD[(VertexId, String)] = users
      .map(user => (toID(user), user))

	// Get movies
    val movies: RDD[(Iterable[String])] = reviews
      .groupBy(_.movieId)
      .map { case (movieId, reviews) => reviews.map(_.userId) }
	  
	// Create edges between vertices as described in the assignment
    val edges: RDD[Edge[String]] =
      movies.map {
	  // Create an edge between all pairs, then delete self loops and duplicated edges
        users => users.flatMap(user1 => users.map(user2 => Edge(toID(user1), toID(user2), "noName")))
      }.flatMap(edges => edges.map(edge => edge)) 
        .distinct()
        .filter(x => x.srcId != x.dstId)
      Graph(vertices, edges)
  }

  def execute(inputFile : String) : Unit = {
    val topK : Int = 100
	// Create the Graph
    val graph = CreateGraph(inputFile)
	// Run PageRank algorithm
    val ranks = graph.pageRank(0.0001).vertices

    // sort by rank and then by id lexicographical order
    val ordering : Ordering[(VertexId, Double)] =
      Ordering.by(vertex => (-vertex._2, vertex._1))

    // get top 100 users after sorting
    val topUsers: Array[(VertexId, Double)] = ranks.takeOrdered(topK)(ordering)

    // print data in the format: User Id: userId, Rank: UserRank

    val asString =  topUsers.map(user => "User Id: " + graph.vertices.filter{case(id, name) => user._1 == toID(name)}.first()._2 + ", Rank: " + user._2)
              .mkString("\n")

    println(asString)

    println()
  }
}

