package univ.bigdata.course;

import univ.bigdata.course.internal.movie.Helpfulness;
import univ.bigdata.course.internal.movie.InternalMovie;
import univ.bigdata.course.internal.movie.InternalMovieReview;
import univ.bigdata.course.movie.Movie;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

public class TestBuilders {
    public static InternalMovieReview scoredReview(String movieId, double score) {
        return new InternalMovieReview(movieId, "user", "user", new Helpfulness(1, 1), score, "", "", "");
    }

    public static InternalMovie scoredMovie(String movieId, double score1, double... scores) {
        List<InternalMovieReview> reviews =
                DoubleStream.concat(
                        DoubleStream.of(score1),
                        DoubleStream.of(scores))
                        .mapToObj(score -> scoredReview(movieId, score)).collect(Collectors.toList());
        return new InternalMovie(movieId, reviews);
    }

    public static InternalMovie namedMovie(String movieId) {
        return scoredMovie(movieId, 3 /*some default avg, because a movie must have reviews.*/);
    }

    public static InternalMovie movieManyReviews(String movieId, int numReviews) {
        List<InternalMovieReview> reviews = new ArrayList<>(numReviews);
        for (int i = 0; i < numReviews; i++)
            reviews.add(new InternalMovieReview(movieId, "id", "name", new Helpfulness(1, 2), 0.5, "", "", ""));

        return new InternalMovie(movieId, reviews);
    }

    public static InternalMovie createMovieHelp(String movieId, String user1, Helpfulness helpfulness1, String user2, Helpfulness helpfulness2) {
        List<InternalMovieReview> reviews = new ArrayList<>();
        reviews.add(new InternalMovieReview(movieId,user1, user1,helpfulness1, 0,"","",""));
        reviews.add(new InternalMovieReview(movieId,user2, user2,helpfulness2, 0,"","",""));
        InternalMovie movie = new InternalMovie(movieId, reviews);
        return movie;
    }

    public static InternalMovie createMovieSummary(String movieName, String review1, String review2) {
        List<InternalMovieReview> reviews = new ArrayList<>();
        reviews.add(new InternalMovieReview(movieName,"", "",null, 0,"","",review1));
        reviews.add(new InternalMovieReview(movieName,"", "",null, 0,"","",review2));
        InternalMovie movie = new InternalMovie(movieName, reviews);
        return movie;
    }
}
