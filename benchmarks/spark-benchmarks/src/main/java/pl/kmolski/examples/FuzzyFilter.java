package pl.kmolski.examples;

import org.apache.spark.sql.SparkSession;
import pl.kmolski.examples.fuzzy.CpuFuzzyFilterFunction;
import pl.kmolski.examples.fuzzy.JcudaFuzzyFilterFunction;
import pl.kmolski.examples.fuzzy.filter.FuzzyPredicate;
import pl.kmolski.examples.fuzzy.filter.FuzzyTNorm;
import pl.kmolski.examples.fuzzy.filter.FuzzyTNormFilter;
import pl.kmolski.utils.SparkJobUtils;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FuzzyFilter {

    private static final Map<String, Function<FuzzyTNorm, FuzzyTNormFilter>> MAPPERS =
            Map.of(
                    "cpu", CpuFuzzyFilterFunction::new,
                    "cuda", JcudaFuzzyFilterFunction::new
            );

    private static long performFilter(SparkSession ctx, FuzzyTNormFilter filter, String inputFile, String outputFile) {
        return SparkJobUtils.waitAndReport(() -> {
            var input = ctx.read().option("header", "true").csv(inputFile);
            var filtered = filter.apply(input);
            filtered.write().option("header", "true").csv(outputFile);
            return filtered.count();
        });
    }

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.printf("Usage: %s <mapper> <inputFile> <outputFile> <threshold> [filters...]%n", FuzzyFilter.class.getName());
            System.exit(2);
        }

        var mapperName = args[0];
        var inputFile = args[1];
        var outputFile = args[2];
        var threshold = Float.parseFloat(args[3]);
        var filters = Arrays.stream(args, 4, args.length).map(FuzzyPredicate::new).collect(Collectors.toList());
        var tNorm = new FuzzyTNorm(threshold, filters);
        var filter = Optional.ofNullable(MAPPERS.get(mapperName)).orElseThrow(
                () -> new IllegalArgumentException("Unknown mapper: " + mapperName)
        ).apply(tNorm);

        System.out.printf("Mapper implementation = %s%n", mapperName);

        try (var ctx = SparkSession.builder().appName("FuzzyFilter").getOrCreate()) {
            System.out.printf("Filtered records count is %s%n", performFilter(ctx, filter, inputFile, outputFile));
        }
    }
}
