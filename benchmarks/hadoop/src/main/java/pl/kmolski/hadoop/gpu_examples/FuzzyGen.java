package pl.kmolski.hadoop.gpu_examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import pl.kmolski.hadoop.gpu_examples.fuzzy.CpuFuzzyGenMapper;
import pl.kmolski.utils.HadoopJobUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class FuzzyGen {

    public static final int RECORD_SIZE = 256;

    private static final String RECORD_COUNT = "mapreduce.fuzzygen.record-count";
    private static final Map<String, Class<? extends Mapper<?, ?, ?, ?>>> MAPPERS =
            Map.of(
                    "cpu", CpuFuzzyGenMapper.class
            );

    private static class RangeInputFormat extends InputFormat<LongWritable, NullWritable> {
        private static class RangeInputSplit extends InputSplit implements Writable {

            private long firstRow;
            private long rowCount;

            public RangeInputSplit(long offset, long length) {
                firstRow = offset;
                rowCount = length;
            }

            public long getLength() {
                return 0;
            }

            public String[] getLocations() {
                return new String[]{};
            }

            public void readFields(DataInput in) throws IOException {
                firstRow = WritableUtils.readVLong(in);
                rowCount = WritableUtils.readVLong(in);
            }

            public void write(DataOutput out) throws IOException {
                WritableUtils.writeVLong(out, firstRow);
                WritableUtils.writeVLong(out, rowCount);
            }
        }

        private static class RangeRecordReader extends RecordReader<LongWritable, NullWritable> {

            private long totalRows;
            private LongWritable key = null;

            public RangeRecordReader() {}

            public void initialize(InputSplit split, TaskAttemptContext context) {
                totalRows = ((RangeInputSplit) split).rowCount;
            }

            public void close() {}

            public LongWritable getCurrentKey() {
                return key;
            }

            public NullWritable getCurrentValue() {
                return NullWritable.get();
            }

            public float getProgress() {
                return 0.0f;
            }

            public boolean nextKeyValue() {
                if (key == null) {
                    key = new LongWritable();
                    key.set(totalRows);
                    return true;
                } else {
                    return false;
                }
            }
        }

        public RecordReader<LongWritable, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
            return new RangeRecordReader();
        }

        public List<InputSplit> getSplits(JobContext job) {
            var nRecords = getNumberOfRows(job);
            var nMappers = job.getConfiguration().getInt(MRJobConfig.NUM_MAPS, 1);

            var splits = new ArrayList<InputSplit>();
            long rangeStart = 0;
            for (int i = 0; i < nMappers; ++i) {
                long rangeEnd = (long) Math.ceil(nRecords * (double)(i + 1) / nMappers);
                splits.add(new RangeInputSplit(rangeStart, rangeEnd - rangeStart));
                rangeStart = rangeEnd;
            }
            return splits;
        }
    }

    private static long getNumberOfRows(JobContext job) {
        return job.getConfiguration().getLong(RECORD_COUNT, 1000);
    }

    private static void generateRecords(
            Class<? extends Mapper<?, ?, ?, ?>> mapperClazz, long nRecords, String outputDir
    ) throws Exception {
        var conf = new Configuration();
        var job = Job.getInstance(conf);
        job.setJobName(FuzzyGen.class.getSimpleName());
        job.setJarByClass(FuzzyGen.class);

        job.setInputFormatClass(RangeInputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(BytesWritable.class);

        job.setMapperClass(mapperClazz);
        job.setNumReduceTasks(0); // run as map-only job

        job.getConfiguration().setLong(RECORD_COUNT, nRecords);
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        HadoopJobUtils.waitAndReport(job);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.printf("Usage: %s <mapper> <nRecords> <outputDir>%n", FuzzyGen.class.getName());
            System.exit(2);
        }


        var mapperName = args[0];
        var mapper = Optional.ofNullable(MAPPERS.get(mapperName)).orElseThrow(
                () -> new IllegalArgumentException("Unknown mapper: " + mapperName)
        );
        var nRecords = Long.parseLong(args[1]);
        var outputDir = args[2];

        System.out.printf("Number of records     = %s%n", nRecords);
        System.out.printf("Mapper implementation = %s%n", mapper);
        generateRecords(mapper, nRecords, outputDir);
    }

}
