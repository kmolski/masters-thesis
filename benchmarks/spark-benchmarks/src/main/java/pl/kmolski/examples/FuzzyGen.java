package pl.kmolski.examples;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import pl.kmolski.examples.fuzzy.CpuFuzzyGenFunction;
import pl.kmolski.examples.fuzzy.JcudaFuzzyGenFunction;
import pl.kmolski.utils.SparkJobUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class FuzzyGen {

    private static final Map<String, FlatMapFunction<Long, byte[]>> MAPPERS =
            Map.of(
                    "cpu", CpuFuzzyGenFunction::call,
                    "cuda", JcudaFuzzyGenFunction::call
            );

    private static List<Long> splitRecordRange(long nRecords, int nMaps) {
        var splits = new ArrayList<Long>();
        long rangeStart = 0;
        for (int i = 0; i < nMaps; ++i) {
            long rangeEnd = (long) Math.ceil(nRecords * (double)(i + 1) / nMaps);
            splits.add(rangeEnd - rangeStart);
            rangeStart = rangeEnd;
        }
        return splits;
    }

    private static void generateRecords(JavaRDD<Long> dataSet, String outputDir, FlatMapFunction<Long, byte[]> mapper) {
        SparkJobUtils.waitAndReport(() -> dataSet.flatMap(mapper).saveAsObjectFile(outputDir));
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.printf("Usage: %s <mapper> <nRecords> <outputDir>%n", FuzzyGen.class.getName());
            System.exit(2);
        }

        var mapperName = args[0];
        var mapper = Optional.ofNullable(MAPPERS.get(mapperName)).orElseThrow(
                () -> new IllegalArgumentException("Unknown mapper: " + mapperName)
        );
        long nRecords = Long.parseLong(args[1]);
        var outputDir = args[2];

        System.out.printf("Number of records     = %s%n", nRecords);
        System.out.printf("Mapper implementation = %s%n", mapper);

        try (var ctx = new JavaSparkContext()) {
            int nMaps = SparkJobUtils.getParallelism(ctx, mapperName);
            var mapInputs = splitRecordRange(nRecords, nMaps);
            var dataSet = ctx.parallelize(mapInputs, nMaps);
            generateRecords(dataSet, outputDir, mapper);
        }
    }
}
