package pl.kmolski.examples.fuzzy;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import pl.kmolski.examples.fuzzy.filter.FuzzyTNorm;
import pl.kmolski.examples.fuzzy.filter.FuzzyTNormFilter;

public class CpuFuzzyFilterFunction implements FuzzyTNormFilter {

    private final FuzzyTNorm tNorm;

    public CpuFuzzyFilterFunction(FuzzyTNorm tNorm) {
        this.tNorm = tNorm;
    }

    @Override
    public Dataset<Row> apply(Dataset<Row> dataset) {
        return dataset.filter(tNorm);
    }
}
