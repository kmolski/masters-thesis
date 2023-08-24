package pl.kmolski.examples.fuzzy;

import jcuda.Sizeof;
import pl.kmolski.utils.FuzzyUtils;
import pl.kmolski.utils.JcudaUtils;

import java.util.ArrayList;
import java.util.Iterator;

import static jcuda.driver.JCudaDriver.cuCtxSynchronize;
import static jcuda.jcurand.JCurand.curandGenerateUniform;
import static pl.kmolski.utils.FuzzyUtils.RECORD_SIZE;

public class JcudaFuzzyGenFunction {

    public static Iterator<byte[]> call(Long nRecords) {
        var ctx = JcudaUtils.createCudaContext();
        long floatCount = nRecords * RECORD_SIZE;
        long byteCount = floatCount * Sizeof.FLOAT;

        var randOutput = JcudaUtils.allocateDeviceMemory(byteCount);
        var generator = JcudaUtils.createRandomGenerator();

        curandGenerateUniform(generator, randOutput, floatCount);
        cuCtxSynchronize();

        var results = new ArrayList<byte[]>();
        FuzzyUtils.forEachRecord(randOutput, nRecords, results::add);
        JcudaUtils.freeResources(generator, randOutput);
        return results.iterator();
    }
}
