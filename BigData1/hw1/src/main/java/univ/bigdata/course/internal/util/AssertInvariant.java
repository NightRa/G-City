package univ.bigdata.course.internal.util;

public class AssertInvariant {
    public static <A> A assertInvariant(A a, boolean inv, String errorMessage) {
        if(inv) {
            return a;
        } else {
            throw new IllegalArgumentException(errorMessage);
        }
    }
}
