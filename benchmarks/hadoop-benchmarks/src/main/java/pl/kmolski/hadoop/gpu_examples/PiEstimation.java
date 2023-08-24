/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import pl.kmolski.hadoop.gpu_examples.qmc.AparapiQmcMapper;
import pl.kmolski.hadoop.gpu_examples.qmc.CpuQmcMapper;
import pl.kmolski.hadoop.gpu_examples.qmc.JcudaQmcMapper;
import pl.kmolski.utils.HadoopJobUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

public class PiEstimation {

    private static final String TMP_DIR_PREFIX = PiEstimation.class.getSimpleName();

    private static final Map<String, Class<? extends Mapper<?, ?, ?, ?>>> MAPPERS =
            Map.of(
                    "cpu", CpuQmcMapper.class,
                    "opencl", AparapiQmcMapper.class,
                    "cuda", JcudaQmcMapper.class
            );

    public static class QmcReducer extends Reducer<BooleanWritable, LongWritable, WritableComparable<?>, Writable> {

        private long numInside = 0;
        private long numOutside = 0;

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

        @Override
        public void cleanup(Context context) throws IOException {
            var conf = context.getConfiguration();
            var outDir = new Path(conf.get(FileOutputFormat.OUTDIR));
            var outFile = new Path(outDir, "reduce-out");
            var fileSys = FileSystem.get(conf);
            var writer = SequenceFile.createWriter(
                    fileSys, conf, outFile, LongWritable.class, LongWritable.class, CompressionType.NONE
            );
            writer.append(new LongWritable(numInside), new LongWritable(numOutside));
            writer.close();
        }
    }

    public static BigDecimal estimatePi(
            int numMaps, long numPoints, Path tmpDir, Configuration conf, Class<? extends Mapper<?, ?, ?, ?>> mapperClazz
    ) throws IOException, ClassNotFoundException, InterruptedException {

        var job = Job.getInstance(conf);
        job.setJobName(PiEstimation.class.getSimpleName());
        job.setJarByClass(PiEstimation.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setOutputKeyClass(BooleanWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(mapperClazz);

        job.setReducerClass(QmcReducer.class);
        job.setNumReduceTasks(1);
        job.setSpeculativeExecution(false);

        var inDir = new Path(tmpDir, "in");
        var outDir = new Path(tmpDir, "out");
        FileInputFormat.setInputPaths(job, inDir);
        FileOutputFormat.setOutputPath(job, outDir);

        var fs = FileSystem.get(conf);
        if (fs.exists(tmpDir)) {
            throw new IOException("Tmp directory " + fs.makeQualified(tmpDir)
                    + " already exists.  Please remove it first.");
        }
        if (!fs.mkdirs(inDir)) {
            throw new IOException("Cannot create input directory " + inDir);
        }

        try {
            for (int i = 0; i < numMaps; ++i) {
                var file = new Path(inDir, "part" + i);
                var offset = new LongWritable(i * numPoints);
                var size = new LongWritable(numPoints);
                var writer = SequenceFile.createWriter(fs, conf, file, LongWritable.class, LongWritable.class, CompressionType.NONE);
                try {
                    writer.append(offset, size);
                } finally {
                    writer.close();
                }
                System.out.println("Wrote input for Map #" + i);
            }

            HadoopJobUtils.waitAndReport(job);

            var inFile = new Path(outDir, "reduce-out");
            var numInside = new LongWritable();
            var numOutside = new LongWritable();
            var reader = new SequenceFile.Reader(fs, inFile, conf);
            try {
                reader.next(numInside, numOutside);
            } finally {
                reader.close();
            }

            var numTotal = BigDecimal.valueOf(numMaps).multiply(BigDecimal.valueOf(numPoints));
            return BigDecimal.valueOf(4).setScale(20)
                    .multiply(BigDecimal.valueOf(numInside.get()))
                    .divide(numTotal, RoundingMode.HALF_UP);
        } finally {
            fs.delete(tmpDir, true);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: " + PiEstimation.class.getName() + " <nMaps> <nSamples> <mapper>");
            System.exit(2);
        }

        var nMaps = Integer.parseInt(args[0]);
        var nSamples = Long.parseLong(args[1]);
        var mapperName = args[2];
        long now = System.currentTimeMillis();
        var rand = new Random().nextInt(Integer.MAX_VALUE);
        var tmpDir = new Path(TMP_DIR_PREFIX + "_" + now + "_" + rand);
        var mapper = Optional.ofNullable(MAPPERS.get(mapperName)).orElseThrow(
                () -> new IllegalArgumentException("Unknown mapper: " + mapperName)
        );

        System.out.println("Number of Maps  = " + nMaps);
        System.out.println("Samples per Map = " + nSamples);
        System.out.println("Mapper implementation = " + mapper);

        System.out.println("Estimated value of Pi is "
                + estimatePi(nMaps, nSamples, tmpDir, new Configuration(), mapper));
    }
}
