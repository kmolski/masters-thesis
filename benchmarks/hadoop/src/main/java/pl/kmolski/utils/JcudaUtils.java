package pl.kmolski.utils;

import jcuda.Pointer;
import jcuda.Sizeof;
import jcuda.driver.CUdeviceptr;
import jcuda.driver.CUfunction;
import jcuda.driver.CUmodule;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.stream.IntStream;

import static jcuda.driver.JCudaDriver.*;
import static jcuda.driver.JCudaDriver.cuMemcpyDtoH;

public class JcudaUtils {

    private JcudaUtils() {}

    public static byte[] toNullTerminatedByteArray(InputStream inStream) throws IOException {
        Objects.requireNonNull(inStream);

        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        inStream.transferTo(outStream);
        outStream.write(0);
        return outStream.toByteArray();
    }

    public static long sumShortArray(long size, CUdeviceptr inputArray) throws IOException {

        byte[] ptx = JcudaUtils.toNullTerminatedByteArray(JcudaUtils.class.getResourceAsStream("/CudaReduction.ptx"));

        CUmodule module = new CUmodule();
        cuModuleLoadData(module, ptx);

        CUfunction reduce = new CUfunction();
        cuModuleGetFunction(reduce, module, "reduce_short");

        int blockSizeX = 256;
        int gridSizeX = (int) Math.ceil((double) size / blockSizeX);

        CUdeviceptr outputArray = new CUdeviceptr();
        cuMemAlloc(outputArray, (long) gridSizeX * Sizeof.SHORT);
        Pointer kernelParams = Pointer.to(
                Pointer.to(inputArray),
                Pointer.to(outputArray),
                Pointer.to(new long[]{size})
        );

        cuLaunchKernel(
                reduce,
                gridSizeX, 1, 1,
                blockSizeX, 1, 1,
                blockSizeX * Sizeof.SHORT, null,
                kernelParams, null
        );
        cuCtxSynchronize();

        short[] sums = new short[gridSizeX];
        cuMemcpyDtoH(Pointer.to(sums), outputArray, (long) gridSizeX * Sizeof.SHORT);
        cuMemFree(outputArray);
        return IntStream.range(0, sums.length).map(i -> sums[i]).sum();
    }
}
