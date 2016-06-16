package univ.bigdata.course.part2

case class RecommendationTask(inputFile: String, outputFile: String, users: Vector[String])

object ParseRecommendation {
  def parseRecommendationTask(lines: Seq[String]): Option[RecommendationTask] = {
    lines.filter(_.nonEmpty) match {
      case Seq(input, output, users @ _*) => Some(RecommendationTask(input, output, users.toVector))
      case _ => None
    }
  }
}
