/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pl.kmolski.hadoop.gpu_examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Time;
import pl.kmolski.hadoop.gpu_examples.qmc.AparapiQmcMapper;
import pl.kmolski.hadoop.gpu_examples.qmc.CpuQmcMapper;
import pl.kmolski.hadoop.gpu_examples.qmc.JcudaQmcMapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

/**
 * A map/reduce program that estimates the value of Pi
 * using a quasi-Monte Carlo (qMC) method.
 * Arbitrary integrals can be approximated numerically by qMC methods.
 * In this example,
 * we use a qMC method to approximate the integral $I = \int_S f(x) dx$,
 * where $S=[0,1)^2$ is a unit square,
 * $x=(x_1,x_2)$ is a 2-dimensional point,
 * and $f$ is a function describing the inscribed circle of the square $S$,
 * $f(x)=1$ if $(2x_1-1)^2+(2x_2-1)^2 &lt;= 1$ and $f(x)=0$, otherwise.
 * It is easy to see that Pi is equal to $4I$.
 * So an approximation of Pi is obtained once $I$ is evaluated numerically.
 *
 * There are better methods for computing Pi.
 * We emphasize numerical approximation of arbitrary integrals in this example.
 * For computing many digits of Pi, consider using bbp.
 *
 * The implementation is discussed below.
 *
 * Mapper:
 *   Generate points in a unit square
 *   and then count points inside/outside of the inscribed circle of the square.
 *
 * Reducer:
 *   Accumulate points inside/outside results from the mappers.
 *
 * Let numTotal = numInside + numOutside.
 * The fraction numInside/numTotal is a rational approximation of
 * the value (Area of the circle)/(Area of the square) = $I$,
 * where the area of the inscribed circle is Pi/4
 * and the area of unit square is 1.
 * Finally, the estimated value of Pi is 4(numInside/numTotal).
 */
public class PiEstimation {

  private static final String TMP_DIR_PREFIX = PiEstimation.class.getSimpleName();

  private static final Map<String, Class<? extends Mapper<?, ?, ?, ?>>> MAPPERS = Map.of(
          "cpu", CpuQmcMapper.class,
          "opencl", AparapiQmcMapper.class,
          "cuda", JcudaQmcMapper.class
  );

  /**
   * Reducer class for Pi estimation.
   * Accumulate points inside/outside results from the mappers.
   */
  public static class QmcReducer extends Reducer<BooleanWritable, LongWritable, WritableComparable<?>, Writable> {

    private long numInside = 0;
    private long numOutside = 0;

    /**
     * Accumulate number of points inside/outside results from the mappers.
     * @param isInside Is the points inside?
     * @param values An iterator to a list of point counts
     * @param context dummy, not used here.
     */
    public void reduce(BooleanWritable isInside, Iterable<LongWritable> values, Context context) {
      if (isInside.get()) {
        for (LongWritable val : values) {
          numInside += val.get();
        }
      } else {
        for (LongWritable val : values) {
          numOutside += val.get();
        }
      }
    }

    /**
     * Reduce task done, write output to a file.
     */
    @Override
    public void cleanup(Context context) throws IOException {
      //write output to a file
      Configuration conf = context.getConfiguration();
      Path outDir = new Path(conf.get(FileOutputFormat.OUTDIR));
      Path outFile = new Path(outDir, "reduce-out");
      FileSystem fileSys = FileSystem.get(conf);
      SequenceFile.Writer writer = SequenceFile.createWriter(
          fileSys, conf, outFile, LongWritable.class, LongWritable.class, CompressionType.NONE
      );
      writer.append(new LongWritable(numInside), new LongWritable(numOutside));
      writer.close();
    }
  }

  /**
   * Run a map/reduce job for estimating Pi.
   *
   * @return the estimated value of Pi
   */
  public static BigDecimal estimatePi(
          int numMaps, long numPoints, Path tmpDir, Configuration conf, Class<? extends Mapper<?, ?, ?, ?>> mapperClazz
  ) throws IOException, ClassNotFoundException, InterruptedException {

    Job job = Job.getInstance(conf);
    job.setJobName(PiEstimation.class.getSimpleName());
    job.setJarByClass(PiEstimation.class);

    job.setInputFormatClass(SequenceFileInputFormat.class);

    job.setOutputKeyClass(BooleanWritable.class);
    job.setOutputValueClass(LongWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapperClass(mapperClazz);

    job.setReducerClass(QmcReducer.class);
    job.setNumReduceTasks(1);

    // turn off speculative execution, because DFS doesn't handle multiple writers to the same file.
    job.setSpeculativeExecution(false);

    //setup input/output directories
    final Path inDir = new Path(tmpDir, "in");
    final Path outDir = new Path(tmpDir, "out");
    FileInputFormat.setInputPaths(job, inDir);
    FileOutputFormat.setOutputPath(job, outDir);

    final FileSystem fs = FileSystem.get(conf);
    if (fs.exists(tmpDir)) {
      throw new IOException("Tmp directory " + fs.makeQualified(tmpDir)
          + " already exists.  Please remove it first.");
    }
    if (!fs.mkdirs(inDir)) {
      throw new IOException("Cannot create input directory " + inDir);
    }

    try {
      //generate an input file for each map task
      for(int i=0; i < numMaps; ++i) {
        final Path file = new Path(inDir, "part"+i);
        final LongWritable offset = new LongWritable(i * numPoints);
        final LongWritable size = new LongWritable(numPoints);
        final SequenceFile.Writer writer = SequenceFile.createWriter(
            fs, conf, file,
            LongWritable.class, LongWritable.class, CompressionType.NONE);
        try {
          writer.append(offset, size);
        } finally {
          writer.close();
        }
        System.out.println("Wrote input for Map #"+i);
      }

      //start a map/reduce job
      System.out.println("Starting Job");
      final long startTime = Time.monotonicNow();
      job.waitForCompletion(true);
      if (!job.isSuccessful()) {
        System.out.println("Job " + job.getJobID() + " failed!");
        System.exit(1);
      }
      final double duration = (Time.monotonicNow() - startTime)/1000.0;
      System.out.println("Job Finished in " + duration + " seconds");

      //read outputs
      Path inFile = new Path(outDir, "reduce-out");
      LongWritable numInside = new LongWritable();
      LongWritable numOutside = new LongWritable();
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, inFile, conf);
      try {
        reader.next(numInside, numOutside);
      } finally {
        reader.close();
      }

      //compute estimated value
      final BigDecimal numTotal
          = BigDecimal.valueOf(numMaps).multiply(BigDecimal.valueOf(numPoints));
      return BigDecimal.valueOf(4).setScale(20)
          .multiply(BigDecimal.valueOf(numInside.get()))
          .divide(numTotal, RoundingMode.HALF_UP);
    } finally {
      fs.delete(tmpDir, true);
    }
  }

  /**
   * main method for running it as a stand alone command.
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: "+ PiEstimation.class.getName()+" <nMaps> <nSamples> <mapper>");
      System.exit(2);
    }

    final int nMaps = Integer.parseInt(args[0]);
    final long nSamples = Long.parseLong(args[1]);
    final String mapperName = args[2];
    long now = System.currentTimeMillis();
    int rand = new Random().nextInt(Integer.MAX_VALUE);
    final Path tmpDir = new Path(TMP_DIR_PREFIX + "_" + now + "_" + rand);
    final Class<? extends Mapper<?, ?, ?, ?>> mapper =
            Optional.ofNullable(MAPPERS.get(mapperName)).orElseThrow(
                    () -> new IllegalArgumentException("Unknown mapper: " + mapperName)
            );

    System.out.println("Number of Maps  = " + nMaps);
    System.out.println("Samples per Map = " + nSamples);
    System.out.println("Mapper implementation = " + mapper);

    System.out.println("Estimated value of Pi is "
            + estimatePi(nMaps, nSamples, tmpDir, new Configuration(), mapper));
  }
}
