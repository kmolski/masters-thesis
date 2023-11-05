package pl.kmolski.examples.qmc;

import com.aparapi.Kernel;
import com.aparapi.Range;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AparapiQmcMapper extends Mapper<LongWritable, LongWritable, BooleanWritable, LongWritable> {

    @Override
    public void map(LongWritable offset, LongWritable size, Context context) throws IOException, InterruptedException {
        int isize = (int) size.get();
        int ioffset = (int) offset.get();
        final boolean[] guesses = new boolean[isize];

        float[][] q = new float[isize][];
        int[][] d = new int[isize][];
        for (int i = 0; i < isize; i++) {
            q[i] = new float[63];
            d[i] = new int[63];
        }

        Kernel kernel = new Kernel() {

            private float getRandomPoint(long index, float[] q, int[] d, int base, int maxDigits) {

                float value = 0.0f;
                long k = index;

                for (int i = 0; i < maxDigits; i++) {
                    q[i] = (i == 0 ? 1.0f : q[i - 1]) / base;
                    d[i] = (int) (k % base);
                    k = (k - d[i]) / base;
                    value += d[i] * q[i];
                }

                boolean cont = true;
                for (int i = 0; i < maxDigits; i++) {
                    if (cont) {
                        d[i]++;
                        value += q[i];
                        if (d[i] < base) {
                            cont = false;
                        } else {
                            value -= (i == 0 ? 1.0f : q[i - 1]);
                        }
                    }
                }

                return value;
            }

            @Override
            public void run() {
                final int i = getGlobalId();
                final long indexOffset = i + ioffset;

                final float x = getRandomPoint(indexOffset, q[i], d[i], 2, 63) - 0.5f;
                final float y = getRandomPoint(indexOffset, q[i], d[i], 3, 40) - 0.5f;

                guesses[i] = (x * x + y * y > 0.25f);
            }
        };
        kernel.execute(Range.create(isize));

        long numInside = 0L;
        long numOutside = 0L;
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
