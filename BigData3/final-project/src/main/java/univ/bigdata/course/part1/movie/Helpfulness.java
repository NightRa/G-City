package univ.bigdata.course.part1.movie;

import univ.bigdata.course.util.Doubles;

import java.io.Serializable;
import java.util.Optional;

public final class Helpfulness implements Serializable {
    public final int thumbsUp;
    public final int total;

    public Helpfulness(int thumbsUp, int total) {
        this.thumbsUp = thumbsUp;
        this.total = total;
    }
	// Combines two Helpfulness classes into one
    public static Helpfulness combine(Helpfulness x, Helpfulness y) {
        return new Helpfulness(x.thumbsUp + y.thumbsUp, x.total + y.total);
    }
	// Calculates helpfulness ratio
    public Optional<Double> helpfulnessRatio() {
        if (total == 0) return Optional.empty();
        else return Optional.of(Doubles.round((double) thumbsUp / total));
    }
	// Check if two helpfulness classes are equal
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Helpfulness that = (Helpfulness) o;

        if (thumbsUp != that.thumbsUp) return false;
        return total == that.total;

    }
	// Hash function
    @Override
    public int hashCode() {
        int result = thumbsUp;
        result = 31 * result + total;
        return result;
    }
	// Helpfulness Output
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
