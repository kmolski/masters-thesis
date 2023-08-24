package pl.kmolski.utils;

import jcuda.Pointer;
import jcuda.driver.CUdeviceptr;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.function.Consumer;

import static jcuda.driver.JCudaDriver.cuMemcpyDtoH;

public final class FuzzyUtils {

    private static final int SET_SIZE = 4;
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

    public static void generateRandomSets(long nRecords, Consumer<byte[]> consumer) {
        var random = new Random();
        for (int i = 0; i < nRecords; ++i) {
            var bytes = new byte[RECORD_BYTES];
            var buffer = ByteBuffer.wrap(bytes);
            for (int j = 0; j < RECORD_SIZE; ++j) {
                buffer.putFloat(random.nextFloat());
            }

            consumer.accept(bytes);
        }
    }

    public static void performOps(byte[] inputRecord) {
        var buffer = ByteBuffer.wrap(inputRecord);

        var sets = new float[SETS_IN_RECORD * 2][];
        for (int i = 0; i < SETS_IN_RECORD; ++i) {
            var set = sets[i] = new float[SET_SIZE];
            for (int j = 0; j < SET_SIZE; ++j) {
                set[j] = buffer.getFloat();
            }
        }
        for (int i = 0; i < SETS_IN_RECORD; ++i) {
            sets[i + SETS_IN_RECORD] = FuzzyUtils.fuzzyComplement(sets[i]);
        }

        var results = Arrays.stream(sets).map(float[]::clone).toArray(float[][]::new);
        for (int i = 0; i < sets.length; ++i) {
            for (int j = 0; j < sets.length; ++j) {
                if (i != j) {
                    results[i] = FuzzyUtils.fuzzyUnion(results[i], results[j]);
                }
            }
        }

        buffer.rewind();
        for (int i = 0; i < SETS_IN_RECORD; ++i) {
            for (int j = 0; j < SET_SIZE; ++j) {
                buffer.putFloat(results[i][j]);
            }
        }
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
