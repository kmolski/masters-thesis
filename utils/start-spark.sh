#!/bin/sh

sh -c "$HADOOP_HOME/sbin/start-dfs.sh"
sh -c "$SPARK_HOME/sbin/start-all.sh"
