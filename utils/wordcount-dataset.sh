#!/usr/bin/env bash

# usage: wordcount-dataset.sh <local source> <HDFS dest> <dest file bytes>
while cat "$1"; do : ; done | head -c "$3" | hadoop fs -put - "$2"
