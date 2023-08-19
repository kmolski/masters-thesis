package pl.kmolski.hadoop.gpu_examples.fuzzy;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static pl.kmolski.hadoop.gpu_examples.fuzzy.FuzzyUtils.SETS_IN_RECORD;
import static pl.kmolski.hadoop.gpu_examples.fuzzy.FuzzyUtils.SET_SIZE;

public class CpuFuzzyComputeMapper extends Mapper<NullWritable, BytesWritable, NullWritable, BytesWritable> {

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

    @Override
    public void map(NullWritable ignored, BytesWritable value, Context context) throws IOException, InterruptedException {
        var bytes = value.getBytes();
        var buffer = ByteBuffer.wrap(bytes);

        var sets = new float[SETS_IN_RECORD * 2][];
        for (int i = 0; i < SETS_IN_RECORD; ++i) {
            var set = sets[i] = new float[SET_SIZE];
            for (int j = 0; j < SET_SIZE; ++j) {
                set[j] = buffer.getFloat();
            }
        }
        for (int i = 0; i < SETS_IN_RECORD; ++i) {
            sets[i + SETS_IN_RECORD] = fuzzyComplement(sets[i]);
        }

        var results = Arrays.stream(sets).map(float[]::clone).toArray(float[][]::new);
        for (int i = 0; i < sets.length; ++i) {
            for (int j = 0; j < sets.length; ++j) {
                if (i != j) {
                    results[i] = fuzzyUnion(results[i], sets[j]);
                }
            }
        }

        buffer.rewind();
        for (int i = 0; i < SETS_IN_RECORD; ++i) {
            for (int j = 0; j < SET_SIZE; ++j) {
                buffer.putFloat(results[i][j]);
            }
        }
        context.write(NullWritable.get(), new BytesWritable(bytes));
    }
}
