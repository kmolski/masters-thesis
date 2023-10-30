#!/usr/bin/env bash

set -euv -o pipefail

mkdir -p logs

JAR=target/spark-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN_CLASS=pl.kmolski.examples.FuzzyFilter
MASTER=spark://bd-cluster-000:7077
FILTER="370 371 372 373 FH.6000.[ENS] - Energy Signals.Cumulative energy consumption"

for n in $(seq -w 1 4); do
    spark-submit --class "$MAIN_CLASS" --master "$MASTER" "$JAR" cpu /simulation-100M.csv /out/fuzzyfilter-cpu-100M 0.8 "$FILTER" 2>"logs/fuzzyfilter_cpu_100M_$n.err" | tee "logs/fuzzyfilter_cpu_100M_$n.out"
    spark-submit --class "$MAIN_CLASS" --master "$MASTER" "$JAR" cpu /simulation-500M.csv /out/fuzzyfilter-cpu-500M 0.8 "$FILTER" 2>"logs/fuzzyfilter_cpu_500M_$n.err" | tee "logs/fuzzyfilter_cpu_500M_$n.out"

    spark-submit --class "$MAIN_CLASS" --master "$MASTER" "$JAR" cuda /simulation-100M.csv /out/fuzzyfilter-cuda-100M 0.8 "$FILTER" 2>"logs/fuzzyfilter_cuda_100M_$n.err" | tee "logs/fuzzyfilter_cuda_100M_$n.out"
    spark-submit --class "$MAIN_CLASS" --master "$MASTER" "$JAR" cuda /simulation-500M.csv /out/fuzzyfilter-cuda-500M 0.8 "$FILTER" 2>"logs/fuzzyfilter_cuda_500M_$n.err" | tee "logs/fuzzyfilter_cuda_500M_$n.out"

    clear-hdfs-out.sh
done
