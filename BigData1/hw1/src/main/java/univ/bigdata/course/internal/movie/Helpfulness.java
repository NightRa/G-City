package univ.bigdata.course.internal.movie;

import java.util.Optional;

public final class Helpfulness {
    public final int thumbsUp;
    public final int total;

    public Helpfulness(int thumbsUp, int total) {
        this.thumbsUp = thumbsUp;
        this.total = total;
    }

    public static Helpfulness combine(Helpfulness x, Helpfulness y) {
        return new Helpfulness(x.thumbsUp + y.thumbsUp, x.total + y.total);
    }

    public Optional<Double> helpfulnessRatio() {
        if (total == 0) return Optional.empty();
        else return Optional.of((double) thumbsUp / total);
    }

    @Override
    public String toString() {
        return thumbsUp + "/" + total;
    }

    /**
     * String for output of helpfulness.
     **/
    public String show() {
        return thumbsUp + "/" + total;
    }
}
