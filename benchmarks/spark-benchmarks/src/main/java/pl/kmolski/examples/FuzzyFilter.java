package pl.kmolski.examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import pl.kmolski.examples.fuzzy.CpuFuzzyFilterFunction;
import pl.kmolski.examples.fuzzy.filter.FuzzyPredicate;
import pl.kmolski.examples.fuzzy.filter.FuzzyTNorm;
import pl.kmolski.examples.fuzzy.filter.FuzzyTNormFilter;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FuzzyFilter {

    private static final Map<String, Function<FuzzyTNorm, FuzzyTNormFilter>> MAPPERS =
            Map.of(
                    "cpu", CpuFuzzyFilterFunction::new
                    //"cuda", JcudaFuzzyFilterFunction::build
            );

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
            Dataset<Row> input = ctx.read().option("header", "true").csv(inputFile);
//            inputDf.rdd().glom().filter(r -> r).flatMap(mapper, new AgnosticEncoders.RowEncoder());
            Dataset<Row> filteredDf = filter.apply(input);
            filteredDf.write().option("header", "true").csv(outputFile);
        }
    }
}
