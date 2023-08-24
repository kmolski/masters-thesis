package pl.kmolski.spark.gpu_examples.fuzzy;

import pl.kmolski.utils.FuzzyUtils;
import pl.kmolski.utils.JcudaUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class JcudaFuzzyComputeFunction {

    public static Iterator<byte[]> call(List<byte[]> records) throws IOException {
        var ctx = JcudaUtils.createCudaContext();
        var bytes = new ByteArrayOutputStream();
        for (var record : records) {
            bytes.writeBytes(record);
        }

        var output = JcudaUtils.fuzzyPerformOps(bytes.toByteArray(), records.size());
        var results = new ArrayList<byte[]>();
        FuzzyUtils.forEachRecord(output, records.size(), results::add);
        JcudaUtils.freeResources(output);
        return results.iterator();
    }
}
