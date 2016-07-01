package univ.bigdata.course.part1.execution

import java.io.{FileOutputStream, PrintStream}
import java.nio.file.{Files, Paths}

import org.apache.spark.rdd.RDD
import univ.bigdata.course.part1.CommandsTask
import univ.bigdata.course.part1.movie.Movie
import univ.bigdata.course.part1.preprocessing.MovieIO

object ExecuteCommands {
  def execute(commandsTask: CommandsTask): Unit = {
    val inputFile = commandsTask.reviewsFileName

    // ------------------------------------------------------------
    // Read movies
    val movies: RDD[Movie] = MovieIO.readMovies(inputFile).sortBy(_.movieId).cache() // Sort to guarantee consistent ordering for assigning IDs
    movies.count() // Force the computation so that the parallel submissions will share.

    // Execute each command, submit jobs in parallel.
    // Returns a vector of resulting string outputs for each command.
    val outputs = commandsTask.commands.par.map(command => command.execute(movies)).toVector

    // ------------------------------------------------------------
    // Write to file.
    val fileOutput: PrintStream = new PrintStream(new FileOutputStream(commandsTask.outputFile))

    outputs.foreach(fileOutput.print)

    fileOutput.close()
  }
}