#!/usr/bin/env bash

set -euv -o pipefail

mkdir -p logs

JAR=target/hadoop-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN_CLASS=pl.kmolski.examples.PiEstimation

for n in $(seq -w 1 10); do
    hadoop jar "$JAR" "$MAIN_CLASS" 100 10000 opencl 2>"logs/pi_opencl_100x10000_$n.err" | tee "logs/pi_opencl_100x10000_$n.out"

    hadoop jar "$JAR" "$MAIN_CLASS" 100 10000 cpu 2>"logs/pi_cpu_100x10000_$n.err" | tee "logs/pi_cpu_100x10000_$n.out"
    hadoop jar "$JAR" "$MAIN_CLASS" 100 1000000 cpu 2>"logs/pi_cpu_100x1M_$n.err" | tee "logs/pi_cpu_100x1M_$n.out"
    hadoop jar "$JAR" "$MAIN_CLASS" 100 10000000 cpu 2>"logs/pi_cpu_100x10M_$n.err" | tee "logs/pi_cpu_100x10M_$n.out"
    hadoop jar "$JAR" "$MAIN_CLASS" 10 100000000 cpu 2>"logs/pi_cpu_10x100M_$n.err" | tee "logs/pi_cpu_10x100M_$n.out"

    hadoop jar "$JAR" "$MAIN_CLASS" 100 10000 cuda 2>"logs/pi_cuda_100x10000_$n.err" | tee "logs/pi_cuda_100x10000_$n.out"
    hadoop jar "$JAR" "$MAIN_CLASS" 100 1000000 cuda 2>"logs/pi_cuda_100x1M_$n.err" | tee "logs/pi_cuda_100x1M_$n.out"
    hadoop jar "$JAR" "$MAIN_CLASS" 100 10000000 cuda 2>"logs/pi_cuda_100x10M_$n.err" | tee "logs/pi_cuda_100x10M_$n.out"
    hadoop jar "$JAR" "$MAIN_CLASS" 10 100000000 cuda 2>"logs/pi_cuda_10x100M_$n.err" | tee "logs/pi_cuda_10x100M_$n.out"
done
