package pl.kmolski.spark.gpu_examples.fuzzy;

import pl.kmolski.utils.FuzzyUtils;

import java.util.ArrayList;
import java.util.Iterator;

public class CpuFuzzyGenFunction {

    public static Iterator<byte[]> call(Long nRecords) {
        var results = new ArrayList<byte[]>();
        FuzzyUtils.generateRandomSets(nRecords, results::add);
        return results.iterator();
    }
}
