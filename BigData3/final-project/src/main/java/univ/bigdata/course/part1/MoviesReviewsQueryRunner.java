package univ.bigdata.course.part1;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class MoviesReviewsQueryRunner {

    public static void main(String[] args) throws UnsupportedEncodingException, URISyntaxException {
        if (args.length != 2) {
            printUsage();
            System.exit(1);
        } else if (!args[0].startsWith("-inputFile=") || !args[1].startsWith("-outputFile=")) {
            printUsage();
            System.exit(1);
        }

        // Main args checks completed
        String inputFile = args[0].replaceFirst("-inputFile=", "");
        String outputPath = args[1].replaceFirst("-outputFile=", "");

        Path inputPath = Paths.get(MoviesFunctions.class.getResource("/"+inputFile).toURI());

        if(!Files.exists(inputPath)) {
            System.err.println("The input file '" + inputFile + "' cannot be found.");
            System.exit(1);
            // For control flow analysis
            return;
        }

        PrintStream fileOutput;
        try {
            fileOutput = new PrintStream(new FileOutputStream(outputPath));
        } catch (FileNotFoundException e) {
            System.err.println("The output file '" + outputPath + "' cannot be found.");
            System.exit(1);
            // For control flow analysis
            return;
        }

        /*final MoviesProvider provider;
        try {

            provider = Conversion.readMoviesProvider(inputPath);

        } catch(IOException e) {
            System.err.println("Error while reading the input file '" + inputFile + "'" );
            e.printStackTrace();
            System.exit(1);
            return;
        }

        MoviesStorage storage = new MoviesStorage(provider);
        generateOutput(storage, fileOutput);*/
    }

    private static void printUsage() {
        System.out.println("Usage: movies -inputFile=<input file> -outputFile=<output file>");
    }
}
