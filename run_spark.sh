#!/bin/bash

if [ $# -ne 0 ]; then
    echo $0: "usage: source run_spark.sh" 
    exit 1
fi

input1=resources/Posts.xml
input2=resources/Votes.xml

$MY_SPARK_HOME/bin/spark-submit \
    --class SimpleApp \
    --master local[2] \
    target/scala-2.11/stack_analysis_2.11-1.0.jar \
    $input1 $input2 
