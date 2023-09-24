#!/usr/bin/env bash

set -euv -o pipefail

mkdir -p logs

JAR=target/hadoop-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN_CLASS=pl.kmolski.examples.FuzzyGen

for n in $(seq -w 1 4); do
    hadoop jar "$JAR" "$MAIN_CLASS" cpu 10000 /out/fuzzygen-cpu-10K 2>"logs/fuzzygen_cpu_10000_$n.err" | tee "logs/fuzzygen_cpu_10000_$n.out"
    hadoop jar "$JAR" "$MAIN_CLASS" cpu 100000 /out/fuzzygen-cpu-100K 2>"logs/fuzzygen_cpu_100K_$n.err" | tee "logs/fuzzygen_cpu_100K_$n.out"

    hadoop jar "$JAR" "$MAIN_CLASS" cuda 10000 /out/fuzzygen-cuda-10K 2>"logs/fuzzygen_cuda_10000_$n.err" | tee "logs/fuzzygen_cuda_10000_$n.out"
    hadoop jar "$JAR" "$MAIN_CLASS" cuda 100000 /out/fuzzygen-cuda-100K 2>"logs/fuzzygen_cuda_100K_$n.err" | tee "logs/fuzzygen_cuda_100K_$n.out"

    clear-hdfs-out.sh
done
