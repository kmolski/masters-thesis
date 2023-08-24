package pl.kmolski.hadoop.gpu_examples.qmc;

public final class QmcUtils {

    private static final int[] BASES = {2, 3};
    private static final int DIMENSIONS = BASES.length;
    private static final int[] MAX_DIGITS = {63, 40};

    private QmcUtils() {}

    public static double[] getRandomPoint(long index) {
        double[] point = new double[DIMENSIONS];

        double[][] q = new double[DIMENSIONS][];
        int[][] d = new int[DIMENSIONS][];
        for (int i = 0; i < DIMENSIONS; i++) {
            q[i] = new double[MAX_DIGITS[i]];
            d[i] = new int[MAX_DIGITS[i]];
        }

        for (int i = 0; i < DIMENSIONS; i++) {
            long k = index;
            point[i] = 0;

            for (int j = 0; j < MAX_DIGITS[i]; j++) {
                q[i][j] = (j == 0 ? 1.0 : q[i][j - 1]) / BASES[i];
                d[i][j] = (int) (k % BASES[i]);
                k = (k - d[i][j]) / BASES[i];
                point[i] += d[i][j] * q[i][j];
            }

            for (int j = 0; j < MAX_DIGITS[i]; j++) {
                d[i][j]++;
                point[i] += q[i][j];
                if (d[i][j] < BASES[i]) {
                    break;
                }
                d[i][j] = 0;
                point[i] -= (j == 0 ? 1.0 : q[i][j - 1]);
            }
        }

        return point;
    }

    public static boolean isPointOutside(double[] point) {
        final double x = point[0] - 0.5;
        final double y = point[1] - 0.5;

        return x * x + y * y > 0.25;
    }
}
