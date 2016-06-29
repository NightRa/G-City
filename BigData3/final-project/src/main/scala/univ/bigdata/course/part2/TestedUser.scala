package univ.bigdata.course.part2

case class TestedUser (userId : Long, moviesSeen : Option[Seq[Long]], unknownRecommendedMovies : Seq[Long])
