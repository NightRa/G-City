package univ.bigdata.course.part1;

import univ.bigdata.course.part1.movie.Helpfulness;
import univ.bigdata.course.part1.movie.Movie;
import univ.bigdata.course.part1.movie.InternalMovieReview;
import univ.bigdata.course.util.Lazy;
import univ.bigdata.course.util.Maps;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.groupingBy;
import static univ.bigdata.course.util.Doubles.round;
import static univ.bigdata.course.util.AssertInvariant.assertInvariant;
import static univ.bigdata.course.util.Lazy.lazy;
import static univ.bigdata.course.util.Maps.mapValues;


/**
 * Main class which capable to keep all information regarding movies reviews.
 * Has to implements all methods from @{@link univ.bigdata.course.MoviesStorage} interface.
 * Also presents functionality to answer different user queries, such as:
 * <p>
 * 1. Total number of distinct movies reviewed.
 * 2. Total number of distinct users that produces the review.
 * 3. Average review score for all movies.
 * 4. Average review score per single movie.
 * 5. Most popular movie reviewed by at least "K" unique users
 * 6. Word count for movie review, select top "K" words
 * 7. K most helpful users
 */
public final class MoviesFunctions {

    /**
     * Invariant: the list of movies doesn't contain 2 movies with the same movieId.
     **/
    public final List<Movie> movies;
    public final Lazy<Map<String, Movie>> lazyMoviesById;

    /**
     * @param movies - a list of movies doesn't contain 2 movies with the same movieId.
     **/
    public MoviesFunctions(List<Movie> movies) {
        this.movies = movies;
        lazyMoviesById = lazy(() -> groupMoviesById(movies));
    }

    /**
     * Given a list of movies, return a map from movieId to *the* movie which has that movieId.
     * <p>
     * Input invariant: the list of movies doesn't contain 2 movies with the same movieId.
     *
     * @param movies - a list of movies
     * @return - a map from movieId to *the* movie which has that movieId.
     **/
    public static Map<String, Movie> groupMoviesById(List<Movie> movies) {
        Map<String, List<Movie>> moviesById =
                movies.stream()
                        .collect(groupingBy(movie -> movie.movieId));
        // mapValues(moviesById, movieList -> movieList.get(0))
        //                                                  ^ ok because groupingBy always returns nonempty collections.
        return mapValues(moviesById, movieList -> assertInvariant(
                movieList.get(0), // The map's value
                movieList.size() == 1, // invariant that must hold
                "Invariant violated: the list of movies has 2 movies with the same movieId: " + movieList));
    }

    /**
     * Method which calculates total scores average for all movies.
     * <p>
     * Dependencies: Movie.avgScore
     *
     * @return the average
     */
    public double totalMoviesAverageScore() {
        return round(movies.stream().flatMapToDouble(movie -> movie.movieReviews.stream().mapToDouble(review -> review.score)).average().getAsDouble());
    }

    /**
     * Given a movieId, find that movie and return it's average score.
     * returns -1 if there is no movie for the given id.
     * <p>
     * Dependencies: lazyMoviesById, Movie.avgScore
     *
     * @param productId id of the movie to calculate the average score of.
     * @return the movie's average, or -1 if movie not found.
     */
    public double totalMovieAverage(final String productId) {
        Map<String, Movie> moviesById = lazyMoviesById.get();
        Movie maybeMovie = moviesById.get(productId);
        if (maybeMovie == null) {
            return -1;
        } else {
            return round(maybeMovie.avgScore());
        }
    }

    /**
     * For each movie calculates it's average score. List should be sorted
     * by average score in decreasing order and in case of same average tie
     * should be broken by natural order of product id. Only top k movies
     * with highest average should be returned
     *
     * @param topK - number of top movies to return
     * @return - list of movies where each @{@link Movie} includes it's average
     */
    public List<Movie> getTopKMoviesAverage(final long topK) {
        return movies.stream()
                .sorted(Movie.movieScoreDescendingOrder.thenComparing(Movie.movieIdOrder))
                .limit(topK)
                .collect(Collectors.toList());
    }

    /**
     * Finds movie with the highest average score among all available movies.
     * If more than one movie has same average score, then movie with lowest
     * product id ordered lexicographically.
     *
     * @return - the movies record @{@link Movie}
     */
    public Movie movieWithHighestAverage() {
        return movies.stream()
                .min(Movie.movieScoreDescendingOrder.thenComparing(Movie.movieIdOrder))
                .orElse(null);
    }

    /**
     * Returns a list of movies which has average of given percentile.
     * List should be sorted according the average score of movie and in case there
     * are more than one movie with same average score, sort by product id
     * lexicographically in natural order.
     * <p>
     * Dependencies: getTopKMoviesAverage().
     *
     * @param percent - the percentile, value in range between [0..100]
     * @return - movies list
     */
    public List<Movie> getMoviesPercentile(final double percent) {
        return getTopKMoviesAverage((long) Math.ceil((1 - (percent / 100)) * movies.size()));
    }

    /*
     * @return - the product id of most reviewed movie among all movies
     */
    public String mostReviewedProduct() {
        return movies.stream()
                .max(Movie.movieNumReviewsOrder)
                .map(movie -> movie.movieId)
                .orElse(null);
    }

    public Stream<Movie> topKMoviesByNumReviews(final int topK) {
        Comparator<Movie> comp = Movie.movieNumReviewsOrder.reversed().thenComparing(Movie.movieIdOrder);
        return movies.stream()
                .sorted(comp)
                .limit(topK);
    }

    /**
     * Computes reviews count per movie, sorted by reviews count in decreasing order, for movies
     * with same amount of review should be ordered by product id. Returns only top k movies with
     * highest review count.
     *
     * @return - returns map with movies product id and the count of over all reviews assigned to it.
     */
    public Map<String, Long> reviewCountPerMovieTopKMovies(final int topK) {
        LinkedHashMap<String, Long> res = new LinkedHashMap<>();
        topKMoviesByNumReviews(topK)
                .forEachOrdered(movie -> res.put(movie.movieId, (long) movie.numReviews()));
        return res;
    }

    /**
     * Computes most popular movie which has been reviewed by at least
     * numOfUsers (provided as parameter).
     *
     * @param numOfUsers - limit of minimum users which reviewed the movie
     * @return - movie which got highest count of reviews
     */
    public String mostPopularMovieReviewedByKUsers(final int numOfUsers) {
        return movies
                .stream()
                .filter(movie -> movie.numReviews() >= numOfUsers)
                .max(comparing(Movie::avgScore))
                .map(movie -> movie.movieId)
                .orElse(null);
    }

    /**
     * Compute map of words count for top K words
     *
     * @param topK - top k number
     * @return - map where key is the word and value is the number of occurrences
     * of this word in the reviews summary, map ordered by words count in decreasing order.
     */
    public Map<String, Long> moviesReviewWordsCount(final int topK) {
        Stream<String> summaries =
                this.movies.stream().flatMap(movie ->
                                movie.movieReviews.stream()
                                        .map(review -> review.review));
        Stream<String> words =
                summaries
                        .flatMap(summary -> Stream.of(summary.split(" ")));

        Map<String, Long> wordCounts =
                mapValues(
                        words.collect(Collectors.groupingBy(word -> word)),
                        list -> (long) list.size());

        Comparator<String> comp = comparing((String word) -> wordCounts.get(word)).reversed().thenComparing(Comparator.naturalOrder());

        Stream<String> sortedStream = wordCounts.keySet().stream()
                .sorted(comp)
                .limit(topK);

        LinkedHashMap<String, Long> result = new LinkedHashMap<>();

        sortedStream.forEach(word -> result.put(word, wordCounts.get(word)));

        return result;
    }

    /*
     * Compute words count map for top Y most reviewed movies. Map includes top K
     * words.
     *
     * @param topMovies - number of top review movies
     * @param topWords  - number of top words to return
     * @return - map of words to count, ordered by count in decreasing order.
     */
    public Map<String, Long> topYMoviewsReviewTopXWordsCount(final int topMovies, final int topWords) {
        List<Movie> topKMovies = topKMoviesByNumReviews(topMovies).collect(Collectors.toList());
        return new MoviesFunctions(topKMovies).moviesReviewWordsCount(topWords);
    }

    /**
     * Compute top k most helpful users that provided reviews for movies
     *
     * @param k - number of users to return
     * @return - map of users to number of reviews they made. Map ordered by number of reviews
     * in decreasing order.
     */
    public Map<String, Double> topKHelpfullUsers(final int k) {
        Map<String, List<InternalMovieReview>> reviewsByReviewer = movies.stream()
                .flatMap(movie -> movie.movieReviews.stream())
                .collect(Collectors.groupingBy(review -> review.userId));
        Map<String, Double> usersHelpfulness = Maps.mapValuesMaybe(reviewsByReviewer, reviews -> reviews.stream()
                .map(review -> review.helpfulness)
                .reduce(new Helpfulness(0, 0), Helpfulness::combine)
                .helpfulnessRatio());
        // Compare first on #reviews, then by name, lexicographically.
        Comparator<String> userReviewsComparator =
                comparing((String user) -> usersHelpfulness.get(user)).reversed()
                        .thenComparing(userId -> userId);
        Stream<String> topHelpfulUsers = usersHelpfulness.keySet().stream()
                .sorted(userReviewsComparator)
                .limit(k);
        LinkedHashMap<String, Double> res = new LinkedHashMap<>();
        topHelpfulUsers.forEachOrdered(user -> res.put(user, round(usersHelpfulness.get(user))));
        return res;
    }

    /**
     * Total movies count
     */
    public long moviesCount() {    // Yuval implemented this
        return this.movies.size(); // beware.. there is no LONG size.... what to do? ask Artem... btw, 2 billion values is 2 much
    }


}
