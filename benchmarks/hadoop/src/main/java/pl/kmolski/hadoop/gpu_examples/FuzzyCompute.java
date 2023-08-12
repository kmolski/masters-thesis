package pl.kmolski.hadoop.gpu_examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FixedLengthInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import pl.kmolski.hadoop.gpu_examples.fuzzy.CpuFuzzyComputeMapper;
import pl.kmolski.utils.HadoopJobUtils;

import java.util.Map;
import java.util.Optional;

import static pl.kmolski.hadoop.gpu_examples.fuzzy.FuzzyUtils.RECORD_BYTES;

public class FuzzyCompute {

    private static final Map<String, Class<? extends Mapper<?, ?, ?, ?>>> MAPPERS =
            Map.of(
                    "cpu", CpuFuzzyComputeMapper.class
            );

    private static void performOperations(
            Class<? extends Mapper<?, ?, ?, ?>> mapperClazz, String inputDir, String outputDir
    ) throws Exception {
        var conf = new Configuration();
        var job = Job.getInstance(conf);
        job.setJobName(FuzzyCompute.class.getSimpleName());
        job.setJarByClass(FuzzyCompute.class);

        FixedLengthInputFormat.setRecordLength(job.getConfiguration(), RECORD_BYTES);
        job.setInputFormatClass(FixedLengthInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setMapperClass(mapperClazz);
        job.setNumReduceTasks(0); // run as map-only job

        FileInputFormat.setInputPaths(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        HadoopJobUtils.waitAndReport(job);
    }

    public static void main(String[] args) throws Exception {
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
        performOperations(mapper, inputDir, outputDir);
    }
}
