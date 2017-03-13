#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "$0" )" && pwd )"
SCRIPT_NAME=`basename "$0"`
SBT="$SCRIPT_DIR/../build/sbt"

__PROJECT_ID=$1   ; shift
__PUBSUB_TOPIC=$1 ; shift

__MAIN_CLASS="com.github.yuiskw.google.datastore.PubsubMessagePublisher"
$SBT "run-main $__MAIN_CLASS --projectId $__PROJECT_ID --pubsubTopic $__PUBSUB_TOPIC"
