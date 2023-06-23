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

        byte[] ptx1 = JcudaUtils.toNullTerminatedByteArray(getClass().getResourceAsStream("/JCudaQmcMapper.ptx"));

        CUmodule module1 = new CUmodule();
        cuModuleLoadData(module1, ptx1);

        CUfunction qmcMapper = new CUfunction();
        cuModuleGetFunction(qmcMapper, module1, "qmc_mapper");

        CUdeviceptr guessesOutput = new CUdeviceptr();
        cuMemAlloc(guessesOutput, size.get() * Sizeof.SHORT);
        Pointer kernelParams1 = Pointer.to(
                Pointer.to(guessesOutput),
                Pointer.to(new long[]{size.get()}),
                Pointer.to(new long[]{offset.get()})
        );

        int blockSizeX = 256;
        int gridSizeX = (int) Math.ceil((double) size.get() / blockSizeX);
        cuLaunchKernel(
                qmcMapper,
                gridSizeX, 1, 1,
                blockSizeX, 1, 1,
                0, null,
                kernelParams1, null
        );
        cuCtxSynchronize();

        long numOutside = JcudaUtils.sumShortArray(size, guessesOutput);
        long numInside = size.get() - numOutside;

        cuMemFree(guessesOutput);
        context.write(new BooleanWritable(true), new LongWritable(numInside));
        context.write(new BooleanWritable(false), new LongWritable(numOutside));
    }
}
