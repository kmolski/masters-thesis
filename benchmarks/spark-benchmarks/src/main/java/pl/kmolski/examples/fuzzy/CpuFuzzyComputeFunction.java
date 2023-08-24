package pl.kmolski.examples.fuzzy;

import pl.kmolski.utils.FuzzyUtils;

import java.util.Iterator;
import java.util.List;

public class CpuFuzzyComputeFunction {

    public static Iterator<byte[]> call(List<byte[]> records) {
        for (var record : records) {
            FuzzyUtils.performOps(record);
        }
        return records.iterator();
    }
}
