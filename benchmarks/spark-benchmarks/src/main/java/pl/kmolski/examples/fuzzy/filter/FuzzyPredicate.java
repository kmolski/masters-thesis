package pl.kmolski.examples.fuzzy.filter;

import org.apache.spark.sql.Row;

import java.io.Serializable;

public class FuzzyPredicate implements Serializable {

    private final float a;
    private final float b;
    private final float c;
    private final float d;
    private final String column;

    public FuzzyPredicate(String expr) {
        String[] parts = expr.split(" ", 5);
        this.a = Float.parseFloat(parts[0]);
        this.b = Float.parseFloat(parts[1]);
        this.c = Float.parseFloat(parts[2]);
        this.d = Float.parseFloat(parts[3]);
        this.column = parts[4];
    }

    public float getA() {
        return a;
    }

    public float getB() {
        return b;
    }

    public float getC() {
        return c;
    }

    public float getD() {
        return d;
    }

    public String getColumn() {
        return column;
    }

    public float getValue(Row row) {
        return Float.parseFloat(row.getString(row.fieldIndex(this.column)));
    }

    public float getMembership(Row row) {
        float value = getValue(row);
        if (value <= this.a || value >= this.d) {
            return 0;  // out of range
        } else if (value >= this.b && value <= this.c) {
            return 1;  // inside plateau
        } else if (value < this.b) { // (val > a && val < b)
            return (value - this.a) / (this.b - this.a); // left triangle
        } else {  // (val > c && val < d)
            return (this.d - value) / (this.d - this.c);  // right triangle
        }
    }
}
