#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "$0" )" && pwd )"
SCRIPT_NAME=`basename "$0"`

# Include useful functions
source "$SCRIPT_DIR/../dev/common.sh"

__ZONE=$1         ; shift
__CLUSTER_NAME=$1 ; shift
__CLASS=$1        ; shift

__JAR_FILE=$(find "$PROJECT_HOME" -iname "mercari-double-data-pipeline*.jar" | head -n 1)
echo "Submit a JAR file: $__JAR_FILE"
gcloud dataproc jobs submit spark \
  --cluster "$__CLUSTER_NAME" \
  --class "$__CLASS" \
  --jars "$__JAR_FILE" $@

exit $?
