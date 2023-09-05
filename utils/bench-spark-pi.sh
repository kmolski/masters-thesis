#!/usr/bin/env bash

set -euv -o pipefail

mkdir -p logs

JAR=target/spark-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN_CLASS=pl.kmolski.examples.PiEstimation
MASTER=spark://bd-cluster-000:7077

for n in $(seq -w 1 10); do
    spark-submit --class "$MAIN_CLASS" --master "$MASTER" "$JAR" 100 10000 opencl 2>"logs/pi_opencl_100x10000_$n.err" | tee "logs/pi_opencl_100x10000_$n.out"

    spark-submit --class "$MAIN_CLASS" --master "$MASTER" "$JAR" 100 10000 cpu 2>"logs/pi_cpu_100x10000_$n.err" | tee "logs/pi_cpu_100x10000_$n.out"
    spark-submit --class "$MAIN_CLASS" --master "$MASTER" "$JAR" 100 1000000 cpu 2>"logs/pi_cpu_100x1M_$n.err" | tee "logs/pi_cpu_100x1M_$n.out"
    spark-submit --class "$MAIN_CLASS" --master "$MASTER" "$JAR" 100 10000000 cpu 2>"logs/pi_cpu_100x10M_$n.err" | tee "logs/pi_cpu_100x10M_$n.out"
    spark-submit --class "$MAIN_CLASS" --master "$MASTER" "$JAR" 10 100000000 cpu 2>"logs/pi_cpu_10x100M_$n.err" | tee "logs/pi_cpu_10x100M_$n.out"

    spark-submit --class "$MAIN_CLASS" --master "$MASTER" "$JAR" 100 10000 cuda 2>"logs/pi_cuda_100x10000_$n.err" | tee "logs/pi_cuda_100x10000_$n.out"
    spark-submit --class "$MAIN_CLASS" --master "$MASTER" "$JAR" 100 1000000 cuda 2>"logs/pi_cuda_100x1M_$n.err" | tee "logs/pi_cuda_100x1M_$n.out"
    spark-submit --class "$MAIN_CLASS" --master "$MASTER" "$JAR" 100 10000000 cuda 2>"logs/pi_cuda_100x10M_$n.err" | tee "logs/pi_cuda_100x10M_$n.out"
    spark-submit --class "$MAIN_CLASS" --master "$MASTER" "$JAR" 10 100000000 cuda 2>"logs/pi_cuda_10x100M_$n.err" | tee "logs/pi_cuda_10x100M_$n.out"
done
