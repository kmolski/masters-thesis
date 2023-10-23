#!/usr/bin/env bash

set -euv -o pipefail

mkdir -p logs

JAR=target/spark-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN_CLASS=pl.kmolski.examples.FuzzyGen
MASTER=spark://bd-cluster-000:7077

for n in $(seq -w 1 4); do
    spark-submit --class "$MAIN_CLASS" --master "$MASTER" "$JAR" cpu 10000 /out/fuzzygen-cpu-10K 2>"logs/fuzzygen_cpu_10000_$n.err" | tee "logs/fuzzygen_cpu_10000_$n.out"
    spark-submit --class "$MAIN_CLASS" --master "$MASTER" "$JAR" cpu 100000 /out/fuzzygen-cpu-100K 2>"logs/fuzzygen_cpu_100K_$n.err" | tee "logs/fuzzygen_cpu_100K_$n.out"

    spark-submit --class "$MAIN_CLASS" --master "$MASTER" "$JAR" cuda 10000 /out/fuzzygen-cuda-10K 2>"logs/fuzzygen_cuda_10000_$n.err" | tee "logs/fuzzygen_cuda_10000_$n.out"
    spark-submit --class "$MAIN_CLASS" --master "$MASTER" "$JAR" cuda 100000 /out/fuzzygen-cuda-100K 2>"logs/fuzzygen_cuda_100K_$n.err" | tee "logs/fuzzygen_cuda_100K_$n.out"

    clear-hdfs-out.sh
done
