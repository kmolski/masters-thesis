package pl.kmolski.examples.fuzzy.filter;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.List;

public class FuzzyTNorm implements FilterFunction<Row>, Serializable {

    private final float threshold;
    private final List<FuzzyPredicate> membershipList;

    public FuzzyTNorm(float threshold, List<FuzzyPredicate> membershipList) {
        this.threshold = threshold;
        this.membershipList = membershipList;
    }

    public float getThreshold() {
        return threshold;
    }

    public List<FuzzyPredicate> getMembershipList() {
        return membershipList;
    }

    @Override
    public boolean call(Row value) throws Exception {
        return threshold <= membershipList.stream()
                .mapToDouble(p -> (double) p.getMembership(value))
                .min()
                .orElse(0.0);
    }
}
