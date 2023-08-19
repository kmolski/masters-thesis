package pl.kmolski.hadoop.gpu_examples.fuzzy;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import pl.kmolski.utils.HadoopJobUtils;
import pl.kmolski.utils.JcudaUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

public class JcudaFuzzyComputeMapper extends Mapper<NullWritable, BytesWritable, NullWritable, BytesWritable> {

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);

        try {
            var ctx = JcudaUtils.createCudaContext();
            var buffer = ByteBuffer.allocate((int) context.getInputSplit().getLength());
            var nRecords = 0L;
            while (context.nextKeyValue()) {
                buffer.put(context.getCurrentValue().getBytes());
                ++nRecords;
            }

            var output = JcudaUtils.fuzzyPerformOps(buffer.array(), buffer.position(), nRecords);
            FuzzyUtils.forEachRecord(output, nRecords, buf -> HadoopJobUtils.writeByteRecord(context, buf));
            JcudaUtils.freeResources(output);
        } finally {
            cleanup(context);
        }
    }
}
