package pl.kmolski.hadoop.gpu_examples.fuzzy;

public final class FuzzyUtils {

    public static final int SET_SIZE = 4;
    public static final int SETS_IN_RECORD = 64;
    public static final int RECORD_SIZE = SET_SIZE * SETS_IN_RECORD;
    public static final int RECORD_BYTES = RECORD_SIZE * Float.BYTES;

    private FuzzyUtils() {}
}
