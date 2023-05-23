package pl.kmolski.hadoop.gpu_examples.qmc;

import com.aparapi.Kernel;
import com.aparapi.Range;
import com.aparapi.device.Device;
import com.aparapi.device.OpenCLDevice;
import com.aparapi.internal.kernel.KernelManager;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.LinkedHashSet;

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

        System.setProperty("com.aparapi.logLevel", "FINE");
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

        KernelManager kernelManager = KernelManager.instance();
        Kernel kernel = new Kernel() {
            @Override
            public void run() {
                final int i = getGlobalId();
                final long offsetIndex = i + offset.get();

                final double x = getRandomCoordinate(q[i], d[i], offsetIndex, 2, 63) - 0.5;
                final double y = getRandomCoordinate(q[i], d[i], offsetIndex, 3, 40) - 0.5;

                guesses[i] = (x * x + y * y > 0.25);
            }
        };

        LinkedHashSet<Device> gpus = new LinkedHashSet<>(OpenCLDevice.listDevices(Device.TYPE.GPU));
        kernelManager.setPreferredDevices(kernel, gpus);
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
