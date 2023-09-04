#!/bin/sh

sh -c "$HADOOP_HOME/sbin/stop-dfs.sh"
sh -c "$SPARK_HOME/sbin/stop-all.sh"
