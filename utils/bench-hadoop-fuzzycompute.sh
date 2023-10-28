#!/usr/bin/env bash

set -euv -o pipefail

mkdir -p logs

JAR=target/hadoop-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN_CLASS=pl.kmolski.examples.FuzzyCompute

for n in $(seq -w 1 4); do
    hadoop jar "$JAR" "$MAIN_CLASS" cpu /fuzzygen-10K /out/fuzzycompute-cpu-10K 2>"logs/fuzzycompute_cpu_10000_$n.err" | tee "logs/fuzzycompute_cpu_10000_$n.out"
    hadoop jar "$JAR" "$MAIN_CLASS" cpu /fuzzygen-100K /out/fuzzycompute-cpu-100K 2>"logs/fuzzycompute_cpu_100K_$n.err" | tee "logs/fuzzycompute_cpu_100K_$n.out"

    hadoop jar "$JAR" "$MAIN_CLASS" cuda /fuzzygen-10K /out/fuzzycompute-cuda-10K 2>"logs/fuzzycompute_cuda_10000_$n.err" | tee "logs/fuzzycompute_cuda_10000_$n.out"
    hadoop jar "$JAR" "$MAIN_CLASS" cuda /fuzzygen-100K /out/fuzzycompute-cuda-100K 2>"logs/fuzzycompute_cuda_100K_$n.err" | tee "logs/fuzzycompute_cuda_100K_$n.out"

    clear-hdfs-out.sh
done
