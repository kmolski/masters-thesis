package pl.kmolski.hadoop.gpu_examples.qmc;

import jcuda.Pointer;
import jcuda.Sizeof;
import jcuda.driver.*;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import pl.kmolski.utils.JcudaUtils;

import java.io.IOException;

import static jcuda.driver.JCudaDriver.*;

public class JcudaQmcMapper extends Mapper<LongWritable, LongWritable, BooleanWritable, LongWritable> {

    @Override
    public void map(LongWritable offset, LongWritable size, Context context) throws IOException, InterruptedException {

        JCudaDriver.setExceptionsEnabled(true);
        cuInit(0);

        CUdevice device = new CUdevice();
        cuDeviceGet(device, 0);

        CUcontext ctx = new CUcontext();
        cuCtxCreate(ctx, 0, device);

        byte[] ptx = JcudaUtils.toNullTerminatedByteArray(getClass().getResourceAsStream("JCudaQmcMapper.ptx"));

        CUmodule module = new CUmodule();
        cuModuleLoadData(module, ptx);

        CUfunction function = new CUfunction();
        cuModuleGetFunction(function, module, "qmc_mapper");

        CUdeviceptr countsOutput = new CUdeviceptr();
        cuMemAlloc(countsOutput, 2 * Sizeof.LONG);
        Pointer kernelParams = Pointer.to(
                countsOutput,
                Pointer.to(new long[]{offset.get()})
        );

        int blockSizeX = 256;
        int gridSizeX = (int) Math.ceil((double) size.get() / blockSizeX);
        cuLaunchKernel(
                function,
                gridSizeX, 1, 1,
                blockSizeX, 1, 1,
                0, null,
                kernelParams, null
        );

        long[] counts = new long[2];
        cuMemcpyDtoH(Pointer.to(counts), countsOutput, 2 * Sizeof.LONG);

        context.write(new BooleanWritable(true), new LongWritable(counts[1]));
        context.write(new BooleanWritable(false), new LongWritable(counts[0]));
    }
}
