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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import pl.kmolski.hadoop.gpu_examples.wordcount.AparapiTokenizerMapper;
import pl.kmolski.hadoop.gpu_examples.wordcount.CpuTokenizerMapper;
import pl.kmolski.utils.HadoopJobUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class WordCount {

    private static final Map<String, Class<? extends Mapper<?, ?, ?, ?>>> MAPPERS = Map.of(
            "cpu", CpuTokenizerMapper.class,
            "opencl", AparapiTokenizerMapper.class
            //"cuda", JcudaQmcMapper.class
    );

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context ctx) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            ctx.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: wordcount <mapper> <in> [<in>...] <out>");
            System.exit(2);
        }

        final String mapperName = args[0];
        final Class<? extends Mapper<?, ?, ?, ?>> mapper =
                Optional.ofNullable(MAPPERS.get(mapperName)).orElseThrow(
                        () -> new IllegalArgumentException("Unknown mapper: " + mapperName)
                );

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(mapper);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        if (mapperName.equals("opencl") || mapperName.equals("cuda")) {
            conf.set("mapreduce.job.running.map.limit", "1");
            conf.set("mapreduce.job.max.split.locations", "1");
        }

        if (otherArgs.length >= 3) {
            for (int i = 1; i < otherArgs.length - 1; ++i) {
                FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
            }
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        }

        HadoopJobUtils.waitAndReport(job);
    }
}
