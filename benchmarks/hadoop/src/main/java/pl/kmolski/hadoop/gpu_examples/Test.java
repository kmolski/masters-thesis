package pl.kmolski.hadoop.gpu_examples;

import jcuda.Pointer;
import jcuda.Sizeof;
import jcuda.driver.*;
import pl.kmolski.utils.JcudaUtils;

import java.io.IOException;

import static jcuda.driver.JCudaDriver.*;

public class Test {

    public static void main(String[] args) throws IOException {

        long size = 100_000;
        long offset = 0;

        JCudaDriver.setExceptionsEnabled(true);
        cuInit(0);

        CUdevice device = new CUdevice();
        cuDeviceGet(device, 0);

        CUcontext ctx = new CUcontext();
        cuCtxCreate(ctx, 0, device);

        byte[] ptx = JcudaUtils.toNullTerminatedByteArray(Test.class.getResourceAsStream("/JCudaQmcMapper.ptx"));

        CUmodule module = new CUmodule();
        cuModuleLoadData(module, ptx);

        CUfunction function = new CUfunction();
        cuModuleGetFunction(function, module, "qmc_mapper");

        CUdeviceptr guessesOutput = new CUdeviceptr();
        cuMemAlloc(guessesOutput, size * Sizeof.BYTE);
        Pointer kernelParams = Pointer.to(
                guessesOutput,
                Pointer.to(new long[]{size}),
                Pointer.to(new long[]{offset})
        );

        int blockSizeX = 256;
        int gridSizeX = (int) Math.ceil((double) size / blockSizeX);
        cuLaunchKernel(
                function,
                gridSizeX, 1, 1,
                blockSizeX, 1, 1,
                0, null,
                kernelParams, null
        );
        cuCtxSynchronize();

        byte[] guesses = new byte[(int) size];
        cuMemcpyDtoH(Pointer.to(guesses), guessesOutput, size * Sizeof.BYTE);

        long numInside = 0L;
        long numOutside = 0L;
        for (byte isOutside : guesses) {
            if (isOutside == 1) {
                numOutside++;
            } else {
                numInside++;
            }
        }

        cuMemFree(guessesOutput);
        System.out.println("Inside: " + numInside);
        System.out.println("Outside: " + numOutside);
    }
}
