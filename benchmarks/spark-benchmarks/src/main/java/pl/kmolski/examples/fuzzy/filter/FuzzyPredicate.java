package pl.kmolski.examples.fuzzy.filter;

import org.apache.spark.sql.Row;

public class FuzzyPredicate {

    private final float a;
    private final float b;
    private final float c;
    private final float d;
    private final String column;

    public FuzzyPredicate(String expr) {
        String[] parts = expr.split(",", 5);
        this.a = Float.parseFloat(parts[0]);
        this.b = Float.parseFloat(parts[1]);
        this.c = Float.parseFloat(parts[2]);
        this.d = Float.parseFloat(parts[3]);
        this.column = parts[4];
    }

    public float getMembership(Row row) {
        float value = row.getFloat(row.fieldIndex(this.column));
        if (value <= this.a || value >= this.d) {
            return 0;  // out of range
        } else if (value >= this.b && value <= this.c) {
            return 1;  // inside plateau
        } else if (value < this.b) { // (val > a && val < b)
            return (value - a) / (b - a); // left triangle
        } else {  // (val > c && val < d)
            return (d - value) / (d - c);  // right triangle
        }
    }
}
