package univ.bigdata.course.part1.execution

import java.io.{FileOutputStream, PrintStream}
import java.nio.file.{Files, Paths}

import org.apache.spark.rdd.RDD
import univ.bigdata.course.part1.CommandsTask
import univ.bigdata.course.part1.movie.Movie
import univ.bigdata.course.part1.preprocessing.MovieIO

object ExecuteCommands {
  def execute(commandsTask: CommandsTask): Unit = {
    // Check if files exist & Create output stream.
    val inputFile = commandsTask.reviewsFileName

    val inputPath = Paths.get(inputFile)
    val outputPath = Paths.get(commandsTask.outputFile)

    if (!Files.exists(inputPath)) sys.error(s"Input file $inputPath doesn't exist")
    if (!Files.exists(outputPath)) sys.error(s"Output file $outputPath doesn't exist")

    val fileOutput: PrintStream = new PrintStream(new FileOutputStream(commandsTask.outputFile))
    // ------------------------------------------------------------
    // Read movies
    val movies: RDD[Movie] = MovieIO.readMovies(inputFile).cache()

    val outputs: Vector[String] = commandsTask.commands.map(command => command.execute(movies))
    outputs.foreach(fileOutput.print)

    // ------------------------------------------------------------
    // Finilize
    fileOutput.close()
  }
}
