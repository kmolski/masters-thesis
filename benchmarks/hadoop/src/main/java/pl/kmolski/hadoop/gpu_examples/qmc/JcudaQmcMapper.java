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

        byte[] ptx = JcudaUtils.toNullTerminatedByteArray(getClass().getResourceAsStream("/JCudaQmcMapper.ptx"));

        CUmodule module = new CUmodule();
        cuModuleLoadData(module, ptx);

        CUfunction function = new CUfunction();
        cuModuleGetFunction(function, module, "qmc_mapper");

        CUdeviceptr guessesOutput = new CUdeviceptr();
        cuMemAlloc(guessesOutput, size.get() * Sizeof.BYTE);
        Pointer kernelParams = Pointer.to(
                guessesOutput,
                Pointer.to(new long[]{size.get(), offset.get()})
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

        byte[] guesses = new byte[(int) size.get()];
        cuMemcpyDtoH(Pointer.to(guesses), guessesOutput, size.get() * Sizeof.BYTE);
        cuMemFree(guessesOutput);

        long numInside = 0L;
        long numOutside = 0L;
        for (byte isInside : guesses) {
            if (isInside == 0) {
                numOutside++;
            } else {
                numInside++;
            }
        }

        context.write(new BooleanWritable(true), new LongWritable(numInside));
        context.write(new BooleanWritable(false), new LongWritable(numOutside));
    }
}
