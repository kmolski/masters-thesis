package pl.kmolski.spark.gpu_examples;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import pl.kmolski.spark.gpu_examples.qmc.AparapiQmcMapper;
import pl.kmolski.spark.gpu_examples.qmc.CpuQmcFunction;
import pl.kmolski.spark.gpu_examples.qmc.JcudaQmcMapper;
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
                    "opencl", AparapiQmcMapper::call,
                    "cuda", JcudaQmcMapper::call
            );

    public static BigDecimal estimatePi(
            int numMaps, long numPoints, JavaRDD<Tuple2<Long, Long>> dataSet, Function<Tuple2<Long, Long>, Long> mapper
    ) {
        Long numInside = SparkJobUtils.waitAndReport(() -> dataSet.map(mapper).reduce(Long::sum));

        var numTotal = BigDecimal.valueOf(numMaps).multiply(BigDecimal.valueOf(numPoints));
        return BigDecimal.valueOf(4).setScale(20)
                .multiply(BigDecimal.valueOf(numInside))
                .divide(numTotal, RoundingMode.HALF_UP);
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: " + PiEstimation.class.getName() + " <nMaps> <nSamples> <mapper>");
            System.exit(2);
        }

        int nMaps = Integer.parseInt(args[0]);
        long nSamples = Long.parseLong(args[1]);
        var mapperName = args[2];
        var mapper = Optional.ofNullable(MAPPERS.get(mapperName)).orElseThrow(
                () -> new IllegalArgumentException("Unknown mapper: " + mapperName)
        );

        System.out.println("Number of Maps  = " + nMaps);
        System.out.println("Samples per Map = " + nSamples);
        System.out.println("Mapper implementation = " + mapper);

        try (var ctx = new JavaSparkContext()) {
            var mapInputs = IntStream.range(0, nMaps)
                    .mapToObj(i -> Tuple2.apply((long) i, nSamples))
                    .collect(Collectors.toList());
            var dataSet = ctx.parallelize(mapInputs, nMaps);
            System.out.println("Estimated value of Pi is " + estimatePi(nMaps, nSamples, dataSet, mapper));
        }
        spark.stop();
    }
}
