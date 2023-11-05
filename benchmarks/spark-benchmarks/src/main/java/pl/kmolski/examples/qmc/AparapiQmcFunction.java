package pl.kmolski.examples.qmc;

import com.aparapi.Kernel;
import com.aparapi.Range;
import scala.Tuple2;

public class AparapiQmcFunction {

    public static Long call(Tuple2<Long, Long> input) {
        int nSamples = Math.toIntExact(input._2());
        int offset = (int) (input._1() * nSamples);
        final var guesses = new boolean[nSamples];

        var q = new float[nSamples][];
        var d = new int[nSamples][];
        for (int i = 0; i < nSamples; ++i) {
            q[i] = new float[63];
            d[i] = new int[63];
        }

        var kernel = new Kernel() {

            private float getRandomPoint(long index, float[] q, int[] d, int base, int maxDigits) {

                float value = 0.0f;
                long k = index;

                for (int i = 0; i < maxDigits; ++i) {
                    q[i] = (i == 0 ? 1.0f : q[i - 1]) / base;
                    d[i] = (int) (k % base);
                    k = (k - d[i]) / base;
                    value += d[i] * q[i];
                }

                boolean cont = true;
                for (int i = 0; i < maxDigits; ++i) {
                    if (cont) {
                        d[i]++;
                        value += q[i];
                        if (d[i] < base) {
                            cont = false;
                        } else {
                            value -= (i == 0 ? 1.0f : q[i - 1]);
                        }
                    }
                }

                return value;
            }

            @Override
            public void run() {
                final int i = getGlobalId();
                final long indexOffset = i + offset;

                final float x = getRandomPoint(indexOffset, q[i], d[i], 2, 63) - 0.5f;
                final float y = getRandomPoint(indexOffset, q[i], d[i], 3, 40) - 0.5f;

                guesses[i] = (x * x + y * y <= 0.25f);
            }
        };
        kernel.execute(Range.create(nSamples));

        long numInside = 0L;
        for (boolean isInside : guesses) {
            if (isInside) {
                numInside++;
            }
        }
        kernel.dispose();

        return numInside;
    }
}
