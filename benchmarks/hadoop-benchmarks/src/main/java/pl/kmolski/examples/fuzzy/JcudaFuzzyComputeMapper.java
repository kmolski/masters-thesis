package pl.kmolski.examples.fuzzy;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import pl.kmolski.utils.HadoopJobUtils;
import pl.kmolski.utils.FuzzyUtils;
import pl.kmolski.utils.JcudaUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class JcudaFuzzyComputeMapper extends Mapper<NullWritable, BytesWritable, NullWritable, BytesWritable> {

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);

        try {
            var ctx = JcudaUtils.createCudaContext();
            var bytes = new ByteArrayOutputStream();
            var nRecords = 0L;
            while (context.nextKeyValue()) {
                bytes.writeBytes(context.getCurrentValue().getBytes());
                ++nRecords;
            }

            var output = JcudaUtils.fuzzyPerformOps(bytes.toByteArray(), nRecords);
            FuzzyUtils.forEachRecord(output, nRecords, buf -> HadoopJobUtils.writeByteRecord(context, buf));
            JcudaUtils.freeResources(output);
        } finally {
            cleanup(context);
        }
    }
}
