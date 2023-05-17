package org.apache.hadoop.gpu_examples.qmc;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper class for Pi estimation.
 * Generate points in a unit square
 * and then count points inside/outside the inscribed circle of the square.
 */
public class CpuQmcMapper extends Mapper<LongWritable, LongWritable, BooleanWritable, LongWritable> {

    private static final int[] BASES = {2, 3};
    private static final int[] MAX_DIGITS = {63, 40};
    private static final int DIMENSIONS = BASES.length;

    /**
     * 2-dimensional Halton sequence {H(i)},
     * where H(i) is a 2-dimensional point and i >= 1 is the index.
     * Halton sequence is used to generate sample points for Pi estimation.
     */
    public double[] getRandomPoint(long index) {
        double[] point = new double[DIMENSIONS];

        double[][] q = new double[DIMENSIONS][];
        int[][] d = new int[DIMENSIONS][];
        for (int i = 0; i < DIMENSIONS; ++i) {
            q[i] = new double[MAX_DIGITS[i]];
            d[i] = new int[MAX_DIGITS[i]];
        }

        for (int i = 0; i < DIMENSIONS; ++i) {
            point[i] = 0;

            for (int j = 0; j < MAX_DIGITS[i]; j++) {
                q[i][j] = (j == 0 ? 1.0 : q[i][j - 1]) / BASES[i];
                d[i][j] = (int) (index % BASES[i]);
                index = (index - d[i][j]) / BASES[i];
                point[i] += d[i][j] * q[i][j];
            }

            for (int j = 0; j < MAX_DIGITS[i]; j++) {
                d[i][j]++;
                point[i] += q[i][j];
                if (d[i][j] < BASES[i]) {
                    break;
                }
                d[i][j] = 0;
                point[i] -= (j == 0 ? 1.0 : q[i][j - 1]);
            }
        }

        return point;
    }

    /**
     * Map method.
     *
     * @param offset  samples starting from the (offset+1)th sample.
     * @param size    the number of samples for this map
     * @param context output {true-&gt;numInside, false-&gt;numOutside}
     */
    public void map(LongWritable offset, LongWritable size, Context context) throws IOException, InterruptedException {

        long numInside = 0L;
        long numOutside = 0L;

        for (long i = 0; i < size.get(); ++i) {
            final double[] point = getRandomPoint(offset.get() + i);
            final double x = point[0] - 0.5;
            final double y = point[1] - 0.5;

            if (x * x + y * y > 0.25) {
                numOutside++;
            } else {
                numInside++;
            }
        }

        context.write(new BooleanWritable(true), new LongWritable(numInside));
        context.write(new BooleanWritable(false), new LongWritable(numOutside));
    }
}
