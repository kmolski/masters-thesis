package pl.kmolski.utils;

import org.apache.spark.api.java.JavaSparkContext;

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

    public static void waitAndReport(Runnable func) {
        waitAndReport(() -> {
            func.run();
            return null;
        });
    }

    public static int getParallelism(JavaSparkContext ctx, String mapperName) {
        if ("cpu".equals(mapperName)) {
            return ctx.defaultParallelism();
        } else {
            return ctx.defaultParallelism() / 4; // TODO: get the real GPU count
        }
    }
}
