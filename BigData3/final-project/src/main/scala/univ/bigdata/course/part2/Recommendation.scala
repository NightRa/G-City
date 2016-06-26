package univ.bigdata.course.part2

import java.io.{FileOutputStream, PrintStream}
import java.security.MessageDigest

import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.mllib.{LongALS, LongMatrixFactorizationModel, Ranking}
import org.apache.spark.rdd.RDD
import univ.bigdata.course.part1.movie.MovieReview
import univ.bigdata.course.part1.preprocessing.MovieIO
import univ.bigdata.course.part2.Recommendation._
import univ.bigdata.course.part3.ExecuteMap
import univ.bigdata.course.part3.ExecuteMap._

case class Recommendation(userName: String, recommendations: Array[String]) extends Serializable{
  override def toString: String =
    s"Recommendations for $userName:" + recommendations.zipWithIndex.map {
      case (movieID, i) => s"${i + 1}. $movieID"
    }.mkString("\n", "\n", "\n======================================\n")
}

object Recommendation {
  val rank: Int = 150
  val iterations: Int = 15
  val numRecommendations: Int = 10

  def createRatings(reviews: RDD[MovieReview], movieIDs: RDD[(String, Long)], userIDs: RDD[(String, Long)]): RDD[Rating[Long]] = {
    val ratings =
      reviews.map(r => (r.movieId, r)).join(movieIDs).map {
        case (movieIDstr, (r, movieID)) => (r.userId, (r, movieID))
      }.join(userIDs).map {
        case (userIDstr, ((r, movieID), userID)) => Rating(userID, movieID, r.score.toFloat)
      }

    ratings
  }

  def als(ratings: RDD[Rating[Long]]): LongMatrixFactorizationModel = {
    // blocks, blocks, rank, iterations, lambda, false, 1.0
    val model = new LongALS().setRank(rank).setIterations(iterations).run(ratings)
    model
  }

  def normalizeReviews(reviews: RDD[MovieReview]): RDD[MovieReview] = {
    val normalizedMovies = {
      val rawMovies = MovieIO.batchMovieReviews(reviews)
      rawMovies.map { movie =>
        val avgScore = movie.avgScore
        movie.copy(movieReviews = movie.movieReviews.map(_.addToScore(-avgScore)))
      }
    }

    val normalizedReviews = normalizedMovies.coalesce(8, shuffle = true).flatMap(_.movieReviews)
    normalizedReviews
  }

  def execute(task: RecommendationTask) = {
    type MovieID = Long
    type UserID = Long
    type UserName = String
    type MovieName = String
    type MovieScore = Double
    type Index = Long

    val reviewsInputFile = task.inputFile
    val reviews : RDD[MovieReview] = MovieIO.getMovieReviews(reviewsInputFile)
    val normalizedReviews : RDD[MovieReview] = normalizeReviews(reviews).cache()

    val movieNames: RDD[(Index, Iterable[MovieName])] = // sorted by Id(movieName)
      normalizedReviews   // MovieID(hash) => MovieName  -- inverted hash function
        .groupBy(_.movieId)
        .map(_._1)
        .sortBy(toID)
        .zipWithIndex() // RDD[MovieName] => RDD[(MovieName, Index)]
        .groupBy(_._2)  // RDD[(Index, (MovieName, Index))]
        .mapValues(_.map(_._1)) // RDD[(Index, MovieName)]
        .distinct(8)

    // Finished preparation
    val ratings: RDD[Rating[Long]] =
      normalizedReviews
        .map(r => Rating(toID(r.userId), toID(r.movieId), r.score.toFloat))
        .cache()

    // Train using ALS
    val model: LongMatrixFactorizationModel = als(ratings)

    // Users requested to be recommended
    // for each user requested - the movies he has already seen
    val moviesSeenAlready: Map[UserID, Seq[MovieID]] =
      normalizedReviews
        .filter(review => task.users.contains(review.userId))
        .groupBy(r => toID(r.userId))   // RDD[(UserID, MovieReview)]
        .mapValues(_.map(_.movieId).map(toID).toSeq)
        .toLocalIterator
        .toMap

    val userVectors: Seq[(UserName, Array[MovieScore])] =
      task.users.view.map(userName => {
          val userId = toID(userName)
          val featureArray =
            model.userFeatures.lookup(userId).headOption match {
              case None => Array.fill(rank)(0.0)
              case Some(features) => features
            }
          (userName, featureArray)
    })

    val userRecommendations: Seq[Recommendation] = {
      userVectors.map {
        case (userName, userFeatureArray) => {
          val allRecommendations : RDD[(MovieID, MovieScore)] =
            LongMatrixFactorizationModel
            .allRecommendations(userFeatureArray, model.productFeatures)
            .distinct(8)
            .sortBy{case (movieId, score) => movieId}

          val groupedRecommendations : RDD[(Index, Iterable[(MovieID, MovieScore)])] =
            allRecommendations
            .zipWithIndex()
            .groupBy(_._2)
            .mapValues(_.map(_._1))

          val joinedMovies: RDD[(Index, (Iterable[(MovieID, MovieScore)], Iterable[MovieName]))] =
            groupedRecommendations
              .join(movieNames)

          val scoredMovieNames : RDD[(MovieName, MovieScore)]=
            joinedMovies
            .map(_._2)
            .map(tupIterator => (tupIterator._1.head, tupIterator._2.head))
            .map { case ((movieID, movieScore), movieName) => (movieName, movieScore)}

          val validMovies  : RDD[(MovieName, MovieScore)] =
            scoredMovieNames
            .filter{case (movieName, movieScore) =>
                      !moviesSeenAlready.get(toID(userName)).map(_.contains(toID(movieName))).getOrElse(false)}

          val recommendations: Array[String] =
            validMovies
                .sortBy{case (movieName, movieScore) => -movieScore}
                .map{case (movieName, movieScore) => movieName}
                .take(numRecommendations)

          Recommendation(userName, recommendations)
        }
      }
    }
    // Output recommendations
    val fileOutput: PrintStream = new PrintStream(new FileOutputStream(task.outputFile))

    userRecommendations.foreach(fileOutput.print)

    fileOutput.close()
  }

  // ------------------------------------------------------------------

  def hash(text: String): Array[Byte] = {
    val md = MessageDigest.getInstance("SHA-256")
    md.update(text.getBytes("UTF-8"))
    md.digest()
  }

  def toID(s: String): Long = {
    val bytes = hash(s)
    BigInt(bytes).toLong // take the lower 64 bits from the 256 bits.
    // Up to 2^32 unique ids.
  }
}
