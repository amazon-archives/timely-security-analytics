#!/bin/bash

spark-submit \
 --master yarn-client \
 --num-executors 40 \
 --conf spark.executor.cores=2 \
 --conf spark.streaming.receiver.maxRate=3 \
 --conf spark.rdd.compress=true \
 --conf spark.cleaner.ttl=3600 \
 --conf spark.streaming.concurrentJobs=5 \
 --class reinvent.securityanalytics.CloudTrailProfileAnalyzer \
 /home/hadoop/cloudtrailanalysisdemo-1.0-SNAPSHOT-jar-with-dependencies.jar \
 <BUCKET> config/reinventConfig.properties \
  2>&1 > output.txt
