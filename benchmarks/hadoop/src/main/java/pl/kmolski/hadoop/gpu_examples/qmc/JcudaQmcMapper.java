package pl.kmolski.hadoop.gpu_examples.qmc;

import jcuda.Pointer;
import jcuda.Sizeof;
import jcuda.driver.*;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import pl.kmolski.utils.JcudaUtils;

import java.io.IOException;
import java.util.stream.IntStream;

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

        byte[] ptx2 = JcudaUtils.toNullTerminatedByteArray(getClass().getResourceAsStream("/CudaShortReduction.ptx"));

        CUmodule module2 = new CUmodule();
        cuModuleLoadData(module2, ptx2);

        CUfunction reduce = new CUfunction();
        cuModuleGetFunction(reduce, module2, "reduce");

        CUdeviceptr reduceOutput = new CUdeviceptr();
        cuMemAlloc(reduceOutput, (long) gridSizeX * Sizeof.SHORT);
        Pointer kernelParams2 = Pointer.to(
                Pointer.to(guessesOutput),
                Pointer.to(reduceOutput),
                Pointer.to(new long[]{size.get()})
        );

        cuLaunchKernel(
                reduce,
                gridSizeX, 1, 1,
                blockSizeX, 1, 1,
                blockSizeX * Sizeof.SHORT * (blockSizeX <= 32 ? 2 : 1), null,
                kernelParams2, null
        );
        cuCtxSynchronize();

        short[] sums = new short[gridSizeX];
        cuMemcpyDtoH(Pointer.to(sums), reduceOutput, (long) gridSizeX * Sizeof.SHORT);

        long numOutside = IntStream.range(0, sums.length).map(i -> sums[i]).sum();
        long numInside = size.get() - numOutside;

        cuMemFree(guessesOutput);
        cuMemFree(reduceOutput);
        context.write(new BooleanWritable(true), new LongWritable(numInside));
        context.write(new BooleanWritable(false), new LongWritable(numOutside));
    }
}
