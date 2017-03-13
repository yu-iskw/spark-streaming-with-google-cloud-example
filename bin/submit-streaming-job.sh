#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "$0" )" && pwd )"
SCRIPT_NAME=$(basename "$0")
PROJECT_HOME="$SCRIPT_DIR/../"

__CLUSTER_NAME=$1 ; shift
__CLASS=$1        ; shift
__SPARK_ARGS=$@

__JAR_FILE=$(find "$PROJECT_HOME" -iname "spark-streaming-with-google-cloud-example*.jar" | head -n 1)
echo "Submit a JAR file: $__JAR_FILE"
gcloud dataproc jobs submit spark \
  --cluster "$__CLUSTER_NAME" \
  --class "$__CLASS" \
  --jars "$__JAR_FILE" -- $__SPARK_ARGS
