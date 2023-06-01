#!/bin/sh

sh -c "$HADOOP_HOME/sbin/start-dfs.sh"
sh -c "$HADOOP_HOME/sbin/start-yarn.sh"
