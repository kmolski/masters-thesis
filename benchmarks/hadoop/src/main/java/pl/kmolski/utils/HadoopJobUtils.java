package pl.kmolski.utils;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Time;

import java.io.IOException;

public class HadoopJobUtils {

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
        System.out.println("Job Finished in " + duration + " seconds");
    }
}
