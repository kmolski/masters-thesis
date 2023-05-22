#!/bin/sh

sh -c "$HADOOP_HOME/sbin/stop-dfs.sh"
sh -c "$HADOOP_HOME/sbin/stop-yarn.sh"
