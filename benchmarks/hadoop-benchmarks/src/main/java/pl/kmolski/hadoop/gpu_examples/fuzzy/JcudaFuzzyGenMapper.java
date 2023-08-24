package pl.kmolski.hadoop.gpu_examples.fuzzy;

import jcuda.Sizeof;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import pl.kmolski.utils.HadoopJobUtils;
import pl.kmolski.utils.FuzzyUtils;
import pl.kmolski.utils.JcudaUtils;

import java.io.IOException;

import static jcuda.driver.JCudaDriver.cuCtxSynchronize;
import static jcuda.jcurand.JCurand.curandGenerateUniform;
import static pl.kmolski.utils.FuzzyUtils.RECORD_SIZE;

public class JcudaFuzzyGenMapper extends Mapper<LongWritable, NullWritable, NullWritable, BytesWritable> {

    @Override
    public void map(LongWritable key, NullWritable ignored, Context context) throws IOException, InterruptedException {
        var ctx = JcudaUtils.createCudaContext();
        var nRecords = key.get();
        var floatCount = nRecords * RECORD_SIZE;
        var byteCount = floatCount * Sizeof.FLOAT;

        var randOutput = JcudaUtils.allocateDeviceMemory(byteCount);
        var generator = JcudaUtils.createRandomGenerator();

        curandGenerateUniform(generator, randOutput, floatCount);
        cuCtxSynchronize();

        FuzzyUtils.forEachRecord(randOutput, nRecords, buf -> HadoopJobUtils.writeByteRecord(context, buf));
        JcudaUtils.freeResources(generator, randOutput);
    }
}
