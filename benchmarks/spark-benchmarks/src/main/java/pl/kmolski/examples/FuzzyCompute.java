package pl.kmolski.examples;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import pl.kmolski.examples.fuzzy.CpuFuzzyComputeFunction;
import pl.kmolski.examples.fuzzy.JcudaFuzzyComputeFunction;
import pl.kmolski.utils.SparkJobUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class FuzzyCompute {

    private static final Map<String, FlatMapFunction<List<byte[]>, byte[]>> MAPPERS =
            Map.of(
                    "cpu", CpuFuzzyComputeFunction::call,
                    "cuda", JcudaFuzzyComputeFunction::call
            );

    private static void performOperations(JavaRDD<byte[]> dataSet, String outputDir, FlatMapFunction<List<byte[]>, byte[]> mapper) {
        SparkJobUtils.waitAndReport(() -> dataSet.glom().flatMap(mapper).saveAsObjectFile(outputDir));
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.printf("Usage: %s <mapper> <inputDir> <outputDir>%n", FuzzyCompute.class.getName());
            System.exit(2);
        }

        var mapperName = args[0];
        var mapper = Optional.ofNullable(MAPPERS.get(mapperName)).orElseThrow(
                () -> new IllegalArgumentException("Unknown mapper: " + mapperName)
        );
        var inputDir = args[1];
        var outputDir = args[2];

        System.out.printf("Mapper implementation = %s%n", mapper);

        try (var ctx = new JavaSparkContext()) {
            var mapInputs = ctx.<byte[]>objectFile(inputDir);
            performOperations(mapInputs, outputDir, mapper);
        }
    }
}
