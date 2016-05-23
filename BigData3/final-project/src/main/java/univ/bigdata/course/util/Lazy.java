package univ.bigdata.course.util;

import java.util.function.Supplier;

public final class Lazy<A> {
    private A value;
    private final Supplier<A> compute;

    public Lazy(Supplier<A> compute) {
        this.value = null;
        this.compute = compute;
    }

    public A get() {
        if(value == null) {
            this.value = compute.get();
        }
        return value;
    }

    public static <A> Lazy<A> lazy(Supplier<A> compute) {
        return new Lazy<>(compute);
    }
}
