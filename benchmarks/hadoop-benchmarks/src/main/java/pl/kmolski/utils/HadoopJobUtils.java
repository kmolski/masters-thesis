package pl.kmolski.utils;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Time;

import java.io.IOException;

public final class HadoopJobUtils {

    private HadoopJobUtils() {}

    public static void waitAndReport(Job job) throws IOException, InterruptedException, ClassNotFoundException {
        System.out.println("Starting Job");
        final long startTime = Time.monotonicNow();

        job.waitForCompletion(true);
        if (!job.isSuccessful()) {
            System.out.println("Job " + job.getJobID() + " failed!");
            System.exit(1);
        }

        final double duration = (Time.monotonicNow() - startTime) / 1000.0;
        System.out.println("Job finished in: " + duration);
    }

    public static void writeByteRecord(Mapper<?, ?, NullWritable, BytesWritable>.Context context, byte[] buf) {
        try {
            context.write(NullWritable.get(), new BytesWritable(buf));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
