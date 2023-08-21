package pl.kmolski.hadoop.gpu_examples.fuzzy;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CpuFuzzyComputeMapper extends Mapper<NullWritable, BytesWritable, NullWritable, BytesWritable> {

    @Override
    public void map(NullWritable ignored, BytesWritable value, Context context) throws IOException, InterruptedException {
        var bytes = value.getBytes();
        FuzzyUtils.performOps(bytes);
        context.write(NullWritable.get(), new BytesWritable(bytes));
    }
}
