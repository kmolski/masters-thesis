#!/usr/bin/env bash

set -euv -o pipefail

mkdir -p logs

for n in $(seq -w 1 10); do
    hadoop jar target/hadoop-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar pl.kmolski.examples.PiEstimation 100 10000 opencl 2>"logs/pi_opencl_100x10000_$n.err" | tee "logs/pi_opencl_100x10000_$n.out"

    hadoop jar target/hadoop-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar pl.kmolski.examples.PiEstimation 100 10000 cpu 2>"logs/pi_cpu_100x10000_$n.err" | tee "logs/pi_cpu_100x10000_$n.out"
    hadoop jar target/hadoop-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar pl.kmolski.examples.PiEstimation 100 1000000 cpu 2>"logs/pi_cpu_100x1M_$n.err" | tee "logs/pi_cpu_100x1M_$n.out"
    hadoop jar target/hadoop-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar pl.kmolski.examples.PiEstimation 100 10000000 cpu 2>"logs/pi_cpu_100x10M_$n.err" | tee "logs/pi_cpu_100x10M_$n.out"

    hadoop jar target/hadoop-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar pl.kmolski.examples.PiEstimation 100 10000 cuda 2>"logs/pi_cuda_100x10000_$n.err" | tee "logs/pi_cuda_100x10000_$n.out"
    hadoop jar target/hadoop-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar pl.kmolski.examples.PiEstimation 100 1000000 cuda 2>"logs/pi_cuda_100x1M_$n.err" | tee "logs/pi_cuda_100x1M_$n.out"
    hadoop jar target/hadoop-benchmarks-1.0-SNAPSHOT-jar-with-dependencies.jar pl.kmolski.examples.PiEstimation 100 10000000 cuda 2>"logs/pi_cuda_100x10M_$n.err" | tee "logs/pi_cuda_100x10M_$n.out"
done
