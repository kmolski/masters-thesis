package pl.kmolski.examples.fuzzy;

import jcuda.Pointer;
import jcuda.Sizeof;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pl.kmolski.examples.fuzzy.filter.FuzzyPredicate;
import pl.kmolski.examples.fuzzy.filter.FuzzyTNorm;
import pl.kmolski.examples.fuzzy.filter.FuzzyTNormFilter;
import pl.kmolski.utils.JcudaUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static jcuda.driver.JCudaDriver.cuMemcpyDtoH;

public class JcudaFuzzyFilterFunction implements FuzzyTNormFilter, MapPartitionsFunction<Row, Row> {

    private static final int A = 0;
    private static final int B = 1;
    private static final int C = 2;
    private static final int D = 3;

    private final float threshold;
    private final float[] memberFnParams;
    private final String[] column;

    public JcudaFuzzyFilterFunction(FuzzyTNorm tNorm) {
        var membershipList = tNorm.getMembershipList();
        int nFilters = membershipList.size();
        this.memberFnParams = new float[nFilters * 4];
        this.column = new String[nFilters];
        for (int i = 0; i < nFilters; ++i) {
            fillMembershipFnParams(i, membershipList.get(i));
            this.column[i] = membershipList.get(i).getColumn();
        }

        this.threshold = tNorm.getThreshold();
    }

    @Override
    public Dataset<Row> apply(Dataset<Row> dataset) {
        return dataset.mapPartitions(this, dataset.encoder());
    }

    @Override
    public Iterator<Row> call(Iterator<Row> input) throws Exception {
        var ctx = JcudaUtils.createCudaContext();
        var rows = new ArrayList<Row>();
        input.forEachRemaining(rows::add);

        int nRows = rows.size();
        var memberPtr = JcudaUtils.fuzzyFilterRows(getRowValues(rows), memberFnParams, threshold, nRows, column.length);
        short[] member = new short[nRows];
        cuMemcpyDtoH(Pointer.to(member), memberPtr, (long) nRows * Sizeof.SHORT);

        var result = new ArrayList<Row>();
        for (int i = 0; i < nRows; ++i) {
            if (member[i] == 1) {
                result.add(rows.get(i));
            }
        }
        JcudaUtils.freeResources(memberPtr);
        return result.iterator();
    }

    private float[] getRowValues(List<Row> rows) {
        int nRows = rows.size();
        int nFilters = column.length;
        float[] values = new float[nFilters * nRows];

        for (int i = 0; i < nFilters; ++i) {
            for (int j = 0; j < nRows; ++j) {
                var row = rows.get(j);
                var value = row.getString(row.fieldIndex(column[i]));
                values[i * nRows + j] = Float.parseFloat(value);
            }
        }
        return values;
    }

    private void fillMembershipFnParams(int i, FuzzyPredicate fuzzyPredicate) {
        this.memberFnParams[i * 4 + A] = fuzzyPredicate.getA();
        this.memberFnParams[i * 4 + B] = fuzzyPredicate.getB();
        this.memberFnParams[i * 4 + C] = fuzzyPredicate.getC();
        this.memberFnParams[i * 4 + D] = fuzzyPredicate.getD();
    }
}
