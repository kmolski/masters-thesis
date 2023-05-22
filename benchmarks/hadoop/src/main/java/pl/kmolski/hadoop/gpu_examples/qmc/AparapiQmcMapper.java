package pl.kmolski.hadoop.gpu_examples.qmc;

import com.aparapi.Kernel;
import com.aparapi.Range;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

public class AparapiQmcMapper extends CpuQmcMapper {

    @Override
    public void map(LongWritable offset, LongWritable size, Context context) throws IOException, InterruptedException {

        long numInside = 0L;
        long numOutside = 0L;

        final boolean[] points = new boolean[(int) size.get()];
        Kernel kernel = new Kernel() {
            @Override
            public void run() {
                int i = getGlobalId();

                final double[] point = getRandomPoint(offset.get() + i);
                final double x = point[0] - 0.5;
                final double y = point[1] - 0.5;

                points[i] = (x * x + y * y <= 0.25);
            }
        };

        Range range = Range.create((int) size.get());
        kernel.execute(range);

        for (boolean isInside : points) {
            if (isInside) {
                numInside++;
            } else {
                numOutside++;
            }
        }
        kernel.dispose();

        context.write(new BooleanWritable(true), new LongWritable(numInside));
        context.write(new BooleanWritable(false), new LongWritable(numOutside));
    }
}
