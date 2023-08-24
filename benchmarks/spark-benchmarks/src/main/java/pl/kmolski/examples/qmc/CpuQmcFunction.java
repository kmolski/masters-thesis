package pl.kmolski.examples.qmc;

import pl.kmolski.utils.QmcUtils;
import scala.Tuple2;

public class CpuQmcFunction {

    public static Long call(Tuple2<Long, Long> input) {
        long nSamples = input._2();
        long offset = input._1() * nSamples;

        long numInside = 0L;
        for (long i = 0; i < nSamples; ++i) {
            final double[] point = QmcUtils.getRandomPoint(offset + i);
            if (QmcUtils.isPointInside(point)) {
                numInside++;
            }
        }

        return numInside;
    }
}
