package pl.kmolski.utils;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;

public final class SparkJobUtils {

    private SparkJobUtils() {}

    public static <R> R waitAndReport(Supplier<R> func) {
        System.out.println("Starting Job");
        final Instant startTime = Instant.now();

        var result = func.get();
        final double duration = Duration.between(startTime, Instant.now()).toMillis() / 1000.0;
        System.out.println("Job finished in: " + duration);

        return result;
    }

    public static void writeByteRecord() {
    }
}
