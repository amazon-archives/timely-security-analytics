#!/usr/bin/bash
spark-shell \
 --master yarn-client \
 --num-executors 40 \
 --conf spark.executor.cores=2 \
 --jars /home/hadoop/cloudtrailanalysisdemo-1.0-SNAPSHOT-jar-with-dependencies.jar

