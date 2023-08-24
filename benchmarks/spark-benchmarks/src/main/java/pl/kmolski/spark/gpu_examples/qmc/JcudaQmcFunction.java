package pl.kmolski.spark.gpu_examples.qmc;

import pl.kmolski.utils.JcudaUtils;
import scala.Tuple2;

import java.io.IOException;

public class JcudaQmcFunction {

    public static Long call(Tuple2<Long, Long> input) throws IOException {
        long nSamples = input._2();
        long offset = input._1() * nSamples;
        var ctx = JcudaUtils.createCudaContext();

        var guesses = JcudaUtils.qmcGeneratePoints(nSamples, offset);
        long numOutside = JcudaUtils.sumShortArray(guesses, nSamples);
        long numInside = nSamples - numOutside;

        JcudaUtils.freeResources(guesses);
        return numInside;
    }
}
