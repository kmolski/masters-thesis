#! /usr/bin/env bash

set -eu -o pipefail

LOG_FILES=("${@/%/*}")
OUT_FILES=("${LOG_FILES[@]/%/out}")
ERR_FILES=("${LOG_FILES[@]/%/err}")
export LC_NUMERIC="pl_PL.UTF-8"

echo "Real time [s]"
RECORD_PREFIX="Job finished in: " get-log-record.awk $OUT_FILES
echo "CPU time [ms]"
RECORD_PREFIX="CPU time spent \\(ms\\)=" get-log-record.awk $ERR_FILES
echo "Map time [ms]"
RECORD_PREFIX="time spent by all map tasks \\(ms\\)=" get-log-record.awk $ERR_FILES
echo "Reduce time [ms]"
RECORD_PREFIX="time spent by all reduce tasks \\(ms\\)=" get-log-record.awk $ERR_FILES
