package univ.bigdata.course.util;

public class Pair<A, B> {
    public final A fst;
    public final B snd;

    public Pair(A fst, B snd) {
        this.fst = fst;
        this.snd = snd;
    }

    public static <A,B> Pair<A,B> pair(A fst, B snd) {
        return new Pair<>(fst, snd);
    }
}
