package pl.kmolski.hadoop.gpu_examples.qmc;

import com.aparapi.Kernel;
import com.aparapi.Range;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AparapiQmcMapper extends Mapper<LongWritable, LongWritable, BooleanWritable, LongWritable> {

    private double getRandomCoordinate(double[] q, int[] d, long index, int base, int maxDigits) {

        double value = 0.0;
        long k = index;

        for (int j = 0; j < maxDigits; j++) {
            q[j] = (j == 0 ? 1.0 : q[j - 1]) / base;
            d[j] = (int) (k % base);
            k = (k - d[j]) / base;
            value += d[j] * q[j];
        }

        boolean cont = true;
        for (int j = 0; j < maxDigits; j++) {
            if (cont) {
                d[j]++;
                value += q[j];
                if (d[j] < base) {
                    cont = false;
                } else {
                    d[j] = 0;
                    value -= (j == 0 ? 1.0 : q[j - 1]);
                }
            }
        }

        return value;
    }

    @Override
    public void map(LongWritable offset, LongWritable size, Context context) throws IOException, InterruptedException {

        long numInside = 0L;
        long numOutside = 0L;

        int isize = (int) size.get();
        final boolean[] guesses = new boolean[isize];

        double[][] q = new double[isize][];
        int[][] d = new int[isize][];
        for (int i = 0; i < isize; i++) {
            q[i] = new double[63];
            d[i] = new int[63];
        }

        Kernel kernel = new Kernel() {
            @Override
            public void run() {
                int i = getGlobalId();

                final double x = getRandomCoordinate(q[i], d[i], i, 2, 63) - 0.5;
                final double y = getRandomCoordinate(q[i], d[i], i, 3, 40) - 0.5;

                guesses[i] = (x * x + y * y > 0.25);
            }
        };

        Range range = Range.create((int) size.get());
        kernel.execute(range);

        for (boolean isOutside : guesses) {
            if (isOutside) {
                numOutside++;
            } else {
                numInside++;
            }
        }
        kernel.dispose();

        context.write(new BooleanWritable(true), new LongWritable(numInside));
        context.write(new BooleanWritable(false), new LongWritable(numOutside));
    }
}
