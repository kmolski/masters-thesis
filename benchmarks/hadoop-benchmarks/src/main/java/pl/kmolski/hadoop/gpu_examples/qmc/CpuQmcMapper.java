package pl.kmolski.hadoop.gpu_examples.qmc;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import pl.kmolski.utils.QmcUtils;

import java.io.IOException;

public class CpuQmcMapper extends Mapper<LongWritable, LongWritable, BooleanWritable, LongWritable> {

    public void map(LongWritable offset, LongWritable size, Context context) throws IOException, InterruptedException {
        long numInside = 0L;
        long numOutside = 0L;

        for (long i = 0; i < size.get(); ++i) {
            final double[] point = QmcUtils.getRandomPoint(offset.get() + i);
            if (QmcUtils.isPointOutside(point)) {
                numOutside++;
            } else {
                numInside++;
            }
        }

        context.write(new BooleanWritable(true), new LongWritable(numInside));
        context.write(new BooleanWritable(false), new LongWritable(numOutside));
    }
}
