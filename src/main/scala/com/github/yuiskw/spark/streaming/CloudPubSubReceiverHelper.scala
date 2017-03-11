/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.yuiskw.spark.streaming

import com.google.cloud.sparkdemo.CloudPubsubReceiver

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
  * This class is a helper class to make dealing with Google Pub/Sub easy
  */
object CloudPubSubReceiverHelper {

  /**
    * create a DStream getting data from Google Pub/Sub
    *
    * @param ssc Streaming context
    * @param projectId Google Cloud project ID
    * @param numReceivers the number of receivers
    * @param topic Google Pub/Sub topic
    * @param subscription Google Pub/Sub subscription
    */
  def createReceivers(
      ssc: StreamingContext,
      projectId: String,
      numReceivers: Int,
      topic: String,
      subscription: String): DStream[String] = {
    val receivers = (3 to numReceivers).map { i =>
      ssc.receiverStream(new CloudPubsubReceiver(projectId, topic, subscription))
    }
    ssc.union(receivers)
  }
}
