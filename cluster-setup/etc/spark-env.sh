#!/usr/bin/env bash

# - JAVA_HOME, to point towards the default Java Runtime
JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")

# - HADOOP_CONF_DIR, to point Spark towards Hadoop configuration files
HADOOP_CONF_DIR="/usr/local/hadoop/etc/hadoop"

# - SPARK_MASTER_HOST, to bind the master to a different IP address or hostname
SPARK_MASTER_HOST="{{ spark_master }}"
