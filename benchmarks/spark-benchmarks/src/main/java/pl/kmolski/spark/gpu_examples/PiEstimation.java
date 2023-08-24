package pl.kmolski.spark.gpu_examples;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import pl.kmolski.spark.gpu_examples.qmc.AparapiQmcFunction;
import pl.kmolski.spark.gpu_examples.qmc.CpuQmcFunction;
import pl.kmolski.spark.gpu_examples.qmc.JcudaQmcFunction;
import pl.kmolski.utils.SparkJobUtils;
import scala.Tuple2;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PiEstimation {

    private static final Map<String, Function<Tuple2<Long, Long>, Long>> MAPPERS =
            Map.of(
                    "cpu", CpuQmcFunction::call,
                    "opencl", AparapiQmcFunction::call,
                    "cuda", JcudaQmcFunction::call
            );

    private static BigDecimal estimatePi(
            int numMaps, long numPoints, JavaRDD<Tuple2<Long, Long>> dataSet, Function<Tuple2<Long, Long>, Long> mapper
    ) {
        Long numInside = SparkJobUtils.waitAndReport(() -> dataSet.map(mapper).reduce(Long::sum));
        var numTotal = BigDecimal.valueOf(numMaps).multiply(BigDecimal.valueOf(numPoints));
        return BigDecimal.valueOf(4).setScale(20).multiply(BigDecimal.valueOf(numInside)).divide(numTotal, RoundingMode.HALF_UP);
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.printf("Usage: %s <nMaps> <nSamples> <mapper>%n", PiEstimation.class.getName());
            System.exit(2);
        }

        int nMaps = Integer.parseInt(args[0]);
        long nSamples = Long.parseLong(args[1]);
        var mapperName = args[2];
        var mapper = Optional.ofNullable(MAPPERS.get(mapperName)).orElseThrow(
                () -> new IllegalArgumentException("Unknown mapper: " + mapperName)
        );

        System.out.printf("Number of Maps  = %s%n", nMaps);
        System.out.printf("Samples per Map = %s%n", nSamples);
        System.out.printf("Mapper implementation = %s%n", mapper);

        try (var ctx = new JavaSparkContext()) {
            var mapInputs = IntStream.range(0, nMaps)
                    .mapToObj(i -> Tuple2.apply((long) i, nSamples))
                    .collect(Collectors.toList());
            var dataSet = ctx.parallelize(mapInputs, nMaps);
            System.out.printf("Estimated value of Pi is %s%n", estimatePi(nMaps, nSamples, dataSet, mapper));
        }
    }
}
