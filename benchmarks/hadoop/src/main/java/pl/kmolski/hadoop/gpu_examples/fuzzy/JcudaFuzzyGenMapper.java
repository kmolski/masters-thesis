package pl.kmolski.hadoop.gpu_examples.fuzzy;

import jcuda.Pointer;
import jcuda.Sizeof;
import jcuda.driver.CUcontext;
import jcuda.driver.CUdevice;
import jcuda.driver.CUdeviceptr;
import jcuda.driver.JCudaDriver;
import jcuda.jcurand.JCurand;
import jcuda.jcurand.curandGenerator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.ByteBuffer;

import static jcuda.driver.JCudaDriver.*;
import static jcuda.jcurand.JCurand.*;
import static jcuda.jcurand.curandRngType.CURAND_RNG_PSEUDO_DEFAULT;
import static pl.kmolski.hadoop.gpu_examples.fuzzy.FuzzyUtils.RECORD_BYTES;
import static pl.kmolski.hadoop.gpu_examples.fuzzy.FuzzyUtils.RECORD_SIZE;

public class JcudaFuzzyGenMapper extends Mapper<LongWritable, NullWritable, NullWritable, BytesWritable> {

    @Override
    public void map(LongWritable key, NullWritable ignored, Context context) throws IOException, InterruptedException {

        JCudaDriver.setExceptionsEnabled(true);
        JCurand.setExceptionsEnabled(true);
        cuInit(0);

        CUdevice device = new CUdevice();
        cuDeviceGet(device, 0);

        CUcontext ctx = new CUcontext();
        cuCtxCreate(ctx, 0, device);

        var nRecords = key.get();
        var floatCount = nRecords * RECORD_SIZE;
        var byteCount = floatCount * Sizeof.FLOAT;

        var randOutput = new CUdeviceptr();
        cuMemAlloc(randOutput, byteCount);

        var generator = new curandGenerator();
        curandCreateGenerator(generator, CURAND_RNG_PSEUDO_DEFAULT);
        curandSetPseudoRandomGeneratorSeed(generator, System.currentTimeMillis());

        curandGenerateUniform(generator, randOutput, floatCount);
        cuCtxSynchronize();

        var bytes = new byte[(int) byteCount];
        cuMemcpyDtoH(Pointer.to(bytes), randOutput, byteCount);

        var buffer = ByteBuffer.wrap(bytes);
        for (int i = 0; i < byteCount; i += RECORD_BYTES) {
            var writableBuf = new byte[RECORD_BYTES];
            buffer.get(writableBuf, 0, RECORD_BYTES);
            context.write(NullWritable.get(), new BytesWritable(writableBuf));
        }

        curandDestroyGenerator(generator);
        cuMemFree(randOutput);
    }
}