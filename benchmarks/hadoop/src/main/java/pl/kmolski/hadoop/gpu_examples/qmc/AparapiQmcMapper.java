package pl.kmolski.hadoop.gpu_examples.qmc;

import com.aparapi.Kernel;
import com.aparapi.Range;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AparapiQmcMapper extends Mapper<LongWritable, LongWritable, BooleanWritable, LongWritable> {

    @Override
    public void map(LongWritable offset, LongWritable size, Context context) throws IOException, InterruptedException {

        System.setProperty("com.aparapi.logLevel", "FINE");
        long numInside = 0L;
        long numOutside = 0L;

        int isize = (int) size.get();
        int ioffset = (int) offset.get();
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
                final int i = getGlobalId();
                final long offsetIndex = i + ioffset;

                double x = 0.0;
                long kx = offsetIndex;

                for (int j = 0; j < 63; j++) {
                    q[i][j] = (j == 0 ? 1.0 : q[i][j - 1]) / 2;
                    d[i][j] = (int) (kx % 2);
                    kx = (kx - d[i][j]) / 2;
                    x += d[i][j] * q[i][j];
                }

                boolean cont = true;
                for (int j = 0; j < 63; j++) {
                    if (cont) {
                        d[i][j]++;
                        x += q[i][j];
                        if (d[i][j] < 2) {
                            cont = false;
                        } else {
                            d[i][j] = 0;
                            x -= (j == 0 ? 1.0 : q[i][j - 1]);
                        }
                    }
                }
                x -= 0.5;
                // duplicated because Aparapi does not support method calls
                double y = 0.0;
                long ky = offsetIndex;

                for (int j = 0; j < 40; j++) {
                    q[i][j] = (j == 0 ? 1.0 : q[i][j - 1]) / 3;
                    d[i][j] = (int) (ky % 3);
                    ky = (ky - d[i][j]) / 3;
                    y += d[i][j] * q[i][j];
                }

                cont = true;
                for (int j = 0; j < 40; j++) {
                    if (cont) {
                        d[i][j]++;
                        y += q[i][j];
                        if (d[i][j] < 3) {
                            cont = false;
                        } else {
                            d[i][j] = 0;
                            y -= (j == 0 ? 1.0 : q[i][j - 1]);
                        }
                    }
                }
                y -= 0.5;

                guesses[i] = (x * x + y * y > 0.25);
            }
        };

        kernel.execute(Range.create(isize));
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
