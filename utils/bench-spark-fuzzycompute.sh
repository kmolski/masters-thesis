#!/usr/bin/env bash

set -euv -o pipefail

mkdir -p logs

JAR=target/spark-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN_CLASS=pl.kmolski.examples.FuzzyCompute
MASTER=spark://bd-cluster-000:7077

for n in $(seq -w 1 4); do
    spark-submit --class "$MAIN_CLASS" --master "$MASTER" "$JAR" cpu /fuzzygen-10K /out/fuzzycompute-cpu-10K 2>"logs/fuzzycompute_cpu_10000_$n.err" | tee "logs/fuzzycompute_cpu_10000_$n.out"
    spark-submit --class "$MAIN_CLASS" --master "$MASTER" "$JAR" cpu /fuzzygen-100K /out/fuzzycompute-cpu-100K 2>"logs/fuzzycompute_cpu_100K_$n.err" | tee "logs/fuzzycompute_cpu_100K_$n.out"

    spark-submit --class "$MAIN_CLASS" --master "$MASTER" "$JAR" cuda /fuzzygen-10K /out/fuzzycompute-cuda-10K 2>"logs/fuzzycompute_cuda_10000_$n.err" | tee "logs/fuzzycompute_cuda_10000_$n.out"
    spark-submit --class "$MAIN_CLASS" --master "$MASTER" "$JAR" cuda /fuzzygen-100K /out/fuzzycompute-cuda-100K 2>"logs/fuzzycompute_cuda_100K_$n.err" | tee "logs/fuzzycompute_cuda_100K_$n.out"

    clear-hdfs-out.sh
done
