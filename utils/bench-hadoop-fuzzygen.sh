#!/usr/bin/env bash

set -euv -o pipefail

mkdir -p logs

for n in $(seq -w 1 10); do
    hadoop jar target/hadoop-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar pl.kmolski.hadoop.gpu_examples.FuzzyGen cpu 10000 /out/fuzzygen-cpu-10K 2>"logs/fuzzygen_cpu_10000_$n.err" | tee "logs/fuzzygen_cpu_10000_$n.out"
    hadoop jar target/hadoop-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar pl.kmolski.hadoop.gpu_examples.FuzzyGen cpu 100000 /out/fuzzygen-cpu-100K 2>"logs/fuzzygen_cpu_100K_$n.err" | tee "logs/fuzzygen_cpu_100K_$n.out"

    hadoop jar target/hadoop-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar pl.kmolski.hadoop.gpu_examples.FuzzyGen cuda 10000 /out/fuzzygen-cuda-10K 2>"logs/fuzzygen_cuda_10000_$n.err" | tee "logs/fuzzygen_cuda_10000_$n.out"
    hadoop jar target/hadoop-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar pl.kmolski.hadoop.gpu_examples.FuzzyGen cuda 100000 /out/fuzzygen-cuda-100K 2>"logs/fuzzygen_cuda_100K_$n.err" | tee "logs/fuzzygen_cuda_100K_$n.out"

    clear-hdfs-out.sh
done
