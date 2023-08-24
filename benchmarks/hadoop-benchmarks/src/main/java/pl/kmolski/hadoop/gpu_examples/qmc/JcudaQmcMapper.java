package pl.kmolski.hadoop.gpu_examples.qmc;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import pl.kmolski.utils.JcudaUtils;

import java.io.IOException;

public class JcudaQmcMapper extends Mapper<LongWritable, LongWritable, BooleanWritable, LongWritable> {

    @Override
    public void map(LongWritable offset, LongWritable size, Context context) throws IOException, InterruptedException {
        var ctx = JcudaUtils.createCudaContext();

        var guesses = JcudaUtils.qmcGeneratePoints(size.get(), offset.get());
        long numOutside = JcudaUtils.sumShortArray(guesses, size.get());
        long numInside = size.get() - numOutside;

        context.write(new BooleanWritable(true), new LongWritable(numInside));
        context.write(new BooleanWritable(false), new LongWritable(numOutside));
        JcudaUtils.freeResources(guesses);
    }
}
