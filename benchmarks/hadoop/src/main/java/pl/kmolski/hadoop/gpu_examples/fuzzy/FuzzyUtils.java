package pl.kmolski.hadoop.gpu_examples.fuzzy;

import jcuda.Pointer;
import jcuda.driver.CUdeviceptr;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

import static jcuda.driver.JCudaDriver.cuMemcpyDtoH;

public final class FuzzyUtils {

    public static final int SET_SIZE = 4;
    public static final int SETS_IN_RECORD = 64;
    public static final int RECORD_SIZE = SET_SIZE * SETS_IN_RECORD;
    public static final int RECORD_BYTES = RECORD_SIZE * Float.BYTES;

    private FuzzyUtils() {}

    public static float[] fuzzyUnion(float[] target, float[] source) {
        int n = Math.min(target.length, source.length);
        for (int i = 0; i < n; ++i) {
            target[i] = Math.max(target[i], source[i]);
        }
        return target;
    }

    public static float[] fuzzyComplement(float[] set) {
        int n = set.length;
        float[] result = new float[n];
        for (int i = 0; i < n; ++i) {
            result[i] = 1.0f - set[i];
        }
        return result;
    }

    public static void forEachRecord(CUdeviceptr inputRecords, long nRecords, Consumer<byte[]> consumer) {
        long byteCount = nRecords * RECORD_BYTES;
        var bytes = new byte[(int) byteCount];
        cuMemcpyDtoH(Pointer.to(bytes), inputRecords, byteCount);

        var buffer = ByteBuffer.wrap(bytes);
        for (int i = 0; i < nRecords; ++i) {
            var writableBuf = new byte[RECORD_BYTES];
            buffer.get(writableBuf, 0, RECORD_BYTES);
            consumer.accept(writableBuf);
        }
    }
}
