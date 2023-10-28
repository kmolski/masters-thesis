package pl.kmolski.examples.fuzzy.filter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface FuzzyTNormFilter {

    Dataset<Row> apply(Dataset<Row> dataset);
}
