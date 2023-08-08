package pl.kmolski.hadoop.gpu_examples.fuzzy;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import static pl.kmolski.hadoop.gpu_examples.fuzzy.FuzzyConstants.RECORD_SIZE;

public class CpuFuzzyGenMapper extends Mapper<LongWritable, NullWritable, NullWritable, BytesWritable> {

    @Override
    public void map(LongWritable key, NullWritable ignored, Context context) throws IOException, InterruptedException {
        var nRecords = key.get();
        var random = new Random();

        for (int i = 0; i < nRecords; ++i) {
            byte[] bytes = new byte[RECORD_SIZE * Float.BYTES];
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            for (int j = 0; j < RECORD_SIZE; ++j) {
                buffer.putFloat(random.nextFloat());
            }
            context.write(NullWritable.get(), new BytesWritable(bytes));
        }
    }
}
