package pl.kmolski.utils;

import jcuda.NativePointerObject;
import jcuda.Pointer;
import jcuda.Sizeof;
import jcuda.driver.*;
import jcuda.jcurand.JCurand;
import jcuda.jcurand.curandGenerator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.stream.IntStream;

import static jcuda.driver.JCudaDriver.*;
import static jcuda.jcurand.JCurand.*;
import static jcuda.jcurand.curandRngType.CURAND_RNG_PSEUDO_DEFAULT;

public final class JcudaUtils {

    private JcudaUtils() {}

    public static CUcontext createCudaContext() {
        JCudaDriver.setExceptionsEnabled(true);
        JCurand.setExceptionsEnabled(true);
        cuInit(0);

        var device = new CUdevice();
        cuDeviceGet(device, 0);

        var ctx = new CUcontext();
        cuCtxCreate(ctx, 0, device);
        return ctx;
    }

    public static CUdeviceptr allocateDeviceMemory(long size) {
        var deviceMemory = new CUdeviceptr();
        cuMemAlloc(deviceMemory, size);
        return deviceMemory;
    }

    public static curandGenerator createRandomGenerator() {
        var generator = new curandGenerator();
        curandCreateGenerator(generator, CURAND_RNG_PSEUDO_DEFAULT);
        curandSetPseudoRandomGeneratorSeed(generator, System.currentTimeMillis());
        return generator;
    }

    public static void freeResources(NativePointerObject... resources) {
        Objects.requireNonNull(resources);

        for (var res : resources) {
            if (res instanceof curandGenerator) {
                curandDestroyGenerator((curandGenerator) res);
            } else if (res instanceof CUdeviceptr) {
                cuMemFree((CUdeviceptr) res);
            } else {
                throw new IllegalArgumentException(String.format("No handler for resource type: %s", res.getClass().getName()));
            }
        }
    }

    public static byte[] toNullTerminatedByteArray(InputStream inStream) throws IOException {
        Objects.requireNonNull(inStream);

        var outStream = new ByteArrayOutputStream();
        inStream.transferTo(outStream);
        outStream.write(0);
        return outStream.toByteArray();
    }

    public static CUdeviceptr qmcGeneratePoints(long nPoints, long seqOffset) throws IOException {
        byte[] ptx = JcudaUtils.toNullTerminatedByteArray(JcudaUtils.class.getResourceAsStream("/CudaQmcKernel.ptx"));

        var module = new CUmodule();
        cuModuleLoadData(module, ptx);

        var qmcKernel = new CUfunction();
        cuModuleGetFunction(qmcKernel, module, "qmc_mapper");

        int blockSizeX = 256;
        int gridSizeX = (int) Math.ceil((double) nPoints / blockSizeX);

        var guesses = JcudaUtils.allocateDeviceMemory(nPoints * Sizeof.SHORT);
        var kernelParams = Pointer.to(
                Pointer.to(guesses),
                Pointer.to(new long[]{nPoints}),
                Pointer.to(new long[]{seqOffset})
        );

        cuLaunchKernel(
                qmcKernel,
                gridSizeX, 1, 1,
                blockSizeX, 1, 1,
                0, null,
                kernelParams, null
        );
        cuCtxSynchronize();

        return guesses;
    }

    public static long sumShortArray(CUdeviceptr inputArray, long size) throws IOException {
        Objects.requireNonNull(inputArray);
        byte[] ptx = JcudaUtils.toNullTerminatedByteArray(JcudaUtils.class.getResourceAsStream("/CudaReduction.ptx"));

        var module = new CUmodule();
        cuModuleLoadData(module, ptx);

        var reduce = new CUfunction();
        cuModuleGetFunction(reduce, module, "reduce_short");

        int blockSizeX = 256;
        int gridSizeX = (int) Math.ceil((double) size / blockSizeX);

        var partialSums = JcudaUtils.allocateDeviceMemory((long) gridSizeX * Sizeof.SHORT);
        var kernelParams = Pointer.to(
                Pointer.to(inputArray),
                Pointer.to(partialSums),
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

        var sums = new short[gridSizeX];
        cuMemcpyDtoH(Pointer.to(sums), partialSums, (long) gridSizeX * Sizeof.SHORT);
        JcudaUtils.freeResources(partialSums);
        return IntStream.range(0, sums.length).map(i -> sums[i]).sum();
    }
}
