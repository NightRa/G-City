package univ.bigdata.course.internal.preprocessing;

import univ.bigdata.course.internal.Conversion;
import univ.bigdata.course.internal.MoviesFunctions;
import univ.bigdata.course.internal.movie.InternalMovie;
import univ.bigdata.course.internal.movie.InternalMovieReview;
import univ.bigdata.course.internal.preprocessing.internal.MovieIOInternals;
import univ.bigdata.course.providers.MoviesProvider;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MovieIO {
    /**
     * May assume that the file exists.
     **/
    public static List<InternalMovieReview> getMovieReviews(Path inputFilePath) throws IOException {
        Stream<String> reviewsLines = Files.readAllLines(inputFilePath).stream();
        Stream<InternalMovieReview> movieReviews = reviewsLines.map(MovieIOInternals::lineToReview);
        return movieReviews.collect(Collectors.toList());
    }

    public static MoviesFunctions readMoviesFunctions(Path inputFilePath) throws IOException {
        List<InternalMovieReview> movieReviews = MovieIO.getMovieReviews(inputFilePath);
        List<InternalMovie> movies = batchMovieReviews(movieReviews);
        return new MoviesFunctions(movies);
    }

    public static List<InternalMovie> batchMovieReviews(List<InternalMovieReview> reviews) {

        Map<String, List<InternalMovieReview>> reviewsGrouped =
                reviews
                        .stream()
                        .collect(Collectors.groupingBy(r -> r.movieId));
        return reviewsGrouped
                .keySet()
                .stream()
                .map(id -> new InternalMovie(id, reviewsGrouped.get(id)))
                .collect(Collectors.toList());
    }
}
