package pl.kmolski.hadoop.gpu_examples.fuzzy;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import pl.kmolski.utils.HadoopJobUtils;

import java.io.IOException;

public class CpuFuzzyGenMapper extends Mapper<LongWritable, NullWritable, NullWritable, BytesWritable> {

    @Override
    public void map(LongWritable key, NullWritable ignored, Context context) throws IOException, InterruptedException {
        var nRecords = key.get();
        FuzzyUtils.generateRandomSets(nRecords, buf -> HadoopJobUtils.writeByteRecord(context, buf));
    }
}
