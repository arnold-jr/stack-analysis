#!/bin/bash

if [ $# -ne 0 ]; then
    echo $0: "usage: source run_spark.sh" 
    exit 1
fi

input1=spark-stats-data/allPosts
input2=spark-stats-data/allVotes

$SPARK_HOME/bin/spark-submit \
    --class SimpleApp \
    --master local[2] \
    target/scala-2.11/q00-project_2.10-1.0.jar \
    $input1 $input2 
