package pl.kmolski.utils;

import jcuda.Pointer;
import jcuda.Sizeof;
import jcuda.driver.CUdeviceptr;
import jcuda.driver.CUfunction;
import jcuda.driver.CUmodule;
import org.apache.hadoop.io.LongWritable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.stream.IntStream;

import static jcuda.driver.JCudaDriver.*;

public class JcudaUtils {

    private JcudaUtils() {}

    public static byte[] toNullTerminatedByteArray(InputStream inStream) throws IOException {
        Objects.requireNonNull(inStream);

        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        inStream.transferTo(outStream);
        outStream.write(0);
        return outStream.toByteArray();
    }

    public static long sumShortArray(LongWritable size, CUdeviceptr guessesOutput) throws IOException {

        byte[] ptx = toNullTerminatedByteArray(JcudaUtils.class.getResourceAsStream("/CudaShortReduction.ptx"));
        CUmodule module = new CUmodule();
        cuModuleLoadData(module, ptx);

        CUfunction reduce = new CUfunction();
        cuModuleGetFunction(reduce, module, "reduce_short");

        int blockSizeX = getReduceThreads((int) size.get(), 128);
        int gridSizeX = getReduceBlocks((int) size.get(), blockSizeX, 64);
        int sharedMemSize = blockSizeX * Sizeof.SHORT * (blockSizeX <= 32 ? 2 : 1);

        CUdeviceptr reduceOutput = new CUdeviceptr();
        cuMemAlloc(reduceOutput, (long) gridSizeX * Sizeof.SHORT);
        Pointer kernelParams = Pointer.to(
                Pointer.to(guessesOutput),
                Pointer.to(reduceOutput),
                Pointer.to(new long[]{size.get()})
        );

        cuLaunchKernel(
                reduce,
                gridSizeX, 1, 1,
                blockSizeX, 1, 1,
                sharedMemSize, null,
                kernelParams, null
        );
        cuCtxSynchronize();

        short[] sums = new short[gridSizeX];
        cuMemcpyDtoH(Pointer.to(sums), reduceOutput, (long) gridSizeX * Sizeof.SHORT);
        cuMemFree(reduceOutput);

        return IntStream.range(0, sums.length).map(i -> sums[i]).sum();
    }

    public static int getReduceThreads(int count, int maxThreads) {
        return (count < maxThreads * 2) ? nextPow2((count + 1) / 2) : maxThreads;
    }

    public static int getReduceBlocks(int count, int threads, int maxBlocks) {
        int blocks = (count + (threads * 2 - 1)) / (threads * 2);
        return Math.min(maxBlocks, blocks);
    }

    private static int nextPow2(int x) {
        --x;
        x |= x >> 1;
        x |= x >> 2;
        x |= x >> 4;
        x |= x >> 8;
        x |= x >> 16;
        return ++x;
    }
}
