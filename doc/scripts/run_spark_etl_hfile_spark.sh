#!/bin/bash

SPARK_HOME=/opt/spark/spark-2.4.3-bin-hadoop2.7

${SPARK_HOME}/bin/spark-submit \
--master local[2] \
--deploy-mode client \
--class com.setapi.project.analystics.NewUserCount \
--driver-memory 512m \
--executor-memory 512m \
--executor-cores 1 \
--num-executors 2 \
--queue default \
--jars \
${SPARK_HOME}/../etl-hfile-saprk-run-hbase-depends.jars/hbase-annotations-1.4.10.jar,\
${SPARK_HOME}/../etl-hfile-saprk-run-hbase-depends.jars/hbase-mapreduce-2.2.0.jar,\
${SPARK_HOME}/../etl-hfile-saprk-run-hbase-depends.jars/hbase-server-2.2.0.jar,\
${SPARK_HOME}/../etl-hfile-saprk-run-hbase-depends.jars/hbase-client-2.2.0.jar,\
${SPARK_HOME}/../etl-hfile-saprk-run-hbase-depends.jars/hbase-metrics-2.2.0.jar,\
${SPARK_HOME}/../etl-hfile-saprk-run-hbase-depends.jars/hbase-shaded-client-2.2.0.jar,\
${SPARK_HOME}/../etl-hfile-saprk-run-hbase-depends.jars/hbase-common-1.4.10.jar,\
${SPARK_HOME}/../etl-hfile-saprk-run-hbase-depends.jars/hbase-metrics-api-2.2.0.jar,\
${SPARK_HOME}/../etl-hfile-saprk-run-hbase-depends.jars/hbase-shaded-miscellaneous-2.2.1.jar,\
${SPARK_HOME}/../etl-hfile-saprk-run-hbase-depends.jars/hbase-common-1.4.10-tests.jar,\
${SPARK_HOME}/../etl-hfile-saprk-run-hbase-depends.jars/hbase-prefix-tree-1.4.10.jar,\
${SPARK_HOME}/../etl-hfile-saprk-run-hbase-depends.jars/hbase-shaded-netty-2.2.1.jar,\
${SPARK_HOME}/../etl-hfile-saprk-run-hbase-depends.jars/hbase-common-2.2.0.jar,\
${SPARK_HOME}/../etl-hfile-saprk-run-hbase-depends.jars/hbase-procedure-2.2.0.jar,\
${SPARK_HOME}/../etl-hfile-saprk-run-hbase-depends.jars/hbase-shaded-protobuf-2.2.1.jar,\
${SPARK_HOME}/../etl-hfile-saprk-run-hbase-depends.jars/hbase-hadoop2-compat-2.2.0.jar,\
${SPARK_HOME}/../etl-hfile-saprk-run-hbase-depends.jars/hbase-protocol-2.2.0.jar,\
${SPARK_HOME}/../etl-hfile-saprk-run-hbase-depends.jars/hbase-zookeeper-2.2.0.jar,\
${SPARK_HOME}/../etl-hfile-saprk-run-hbase-depends.jars/hbase-hadoop-compat-2.2.0.jar,\
${SPARK_HOME}/../etl-hfile-saprk-run-hbase-depends.jars/hbase-protocol-shaded-2.2.0.jar,\
${SPARK_HOME}/../etl-hfile-saprk-run-hbase-depends.jars/metrics-core-4.1.0.jar,\
${SPARK_HOME}/../etl-hfile-saprk-run-hbase-depends.jars/hbase-http-2.2.0.jar,\
${SPARK_HOME}/../etl-hfile-saprk-run-hbase-depends.jars/hbase-replication-2.2.0.jar,\
${SPARK_HOME}/../etl-hfile-saprk-run-hbase-depends.jars/htrace-core4-4.1.0-incubating.jar\
 \
${SPARK_HOME}/../etl-hfile-saprk.jar \
2015-12-20

