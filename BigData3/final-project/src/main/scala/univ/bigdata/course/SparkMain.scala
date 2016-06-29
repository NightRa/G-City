package univ.bigdata.course

import java.nio.file.{Files, Paths}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import univ.bigdata.course.part1.execution.ExecuteCommands
import univ.bigdata.course.part1.parsing.ParseCommand
import univ.bigdata.course.part2.{ParseRecommendation, Recommendation}
import univ.bigdata.course.part3.ExecuteMap
import univ.bigdata.course.part4.PageRank

import scala.collection.JavaConverters._

object SparkMain {
  lazy val conf = new SparkConf().setAppName("Big Data HW3").setMaster("local[*]")
  lazy val sc = new SparkContext(conf)

  def init(): Unit = {
    // Touch the lazy vals to init them.
    conf
    sc
    Logger.getRootLogger.setLevel(Level.ERROR)
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      invalidArgsError(args)
    }

    args(0) match {
      case "commands" =>
        if (args.length != 2) invalidArgsError(args)
        val commandsFile = Paths.get(args(1))

        val lines = Files.readAllLines(commandsFile) // Read lines, Can throw error here
        val commandsTaskO = ParseCommand.parseCommandsTask(lines.asScala) // Parse commands file
        commandsTaskO match {
          case Left(err) => sys.error("Invalid commands input file format: " + err)
          case Right(commandsTask) => ExecuteCommands.execute(commandsTask) // Execute commands task
        }
      case "recommend" =>

        if (args.length != 2) invalidArgsError(args)
        val recommendFileName = Paths.get(args(1))
        val lines = Files.readAllLines(recommendFileName) // Read lines, Can throw error here
        val recommendTaskO = ParseRecommendation.parseRecommendationTask(lines.asScala)
        recommendTaskO match {
          case Some(task) => Recommendation.execute(task)
          case None => sys.error("Invalid recommend input file format")
        }
      case "map" =>
        if (args.length != 3) invalidArgsError(args)
        val trainFile = args(1)
        val testFile = args(2)
        ExecuteMap.execute(trainFile, testFile)
      case "pagerank" =>
        if (args.length != 2) invalidArgsError(args)
        val moviesFile = args(1)
        PageRank.execute(moviesFile)
      case _ =>
        invalidArgsError(args)
    }

    // TODO: Don't forget to remove this line before submission.
    readLine()

  }

  def invalidArgsError(args: Array[String]): Unit = {
    println("Invalid program arguments.")
    println("Valid args: commands commandsFileName | recommend recommendFileName | map movies-train.txt movies-test.txt | pagerank movies-simple.txt")
    println("Given args: " + args.mkString("[", ",", "]"))
    System.exit(1)
  }
}
