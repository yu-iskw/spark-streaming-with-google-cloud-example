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

import com.github.yuiskw.spark.streaming.UserActionCounter.Params
import scopt.OptionParser

import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


class UserActionStats(userId: Int, numActions: Int) extends Serializable {

  def merge(another: UserActionStats): UserActionStats = {
    require(another.userId == this.userId)
    new UserActionStats(userId, this.numActions + another.numActions)
  }
}


class UserActionStatUpdater(projectId: String, outputKind: String) {

  def apply(
    userId: Int,
    newState: Option[UserActionStats],
    state: State[UserActionStats]): (Int, UserActionStats) = {
    val updated = newState.getOrElse(new UserActionStats(userId, 0))
      .merge(state.getOption().getOrElse(new UserActionStats(userId, 0)))
    val output = (userId, updated)
    state.update(updated)
    output
  }
}


class UserActionCounter {

  val log = Logger.getLogger(this.getClass.getSimpleName)
  log.setLevel(Level.INFO)

  def run(sc: SparkContext, params: Params): Unit = {
    log.info(s"Starting with the parameters: ${params.toString}")

    // get or create a Streaming context
    val ssc = StreamingContext.getOrCreate(
      params.checkpointPath,
      () => {
        val ssc = new StreamingContext(sc, Seconds(params.windowDurationSecs))
        ssc.checkpoint(params.checkpointPath)
        ssc
      })

    // Create a DStream which gets data from Google Pub/Sub
    val subscriptionName = s"${params.inputTopic}_${this.getClass.getSimpleName.toLowerCase}"
    val eventDStream = CloudPubSubReceiverHelper
      .createReceivers(
        ssc,
        params.projectId,
        params.numReceivers,
        params.inputTopic,
        subscriptionName)
      .repartition(params.numPartitions)
      .window(Seconds(params.windowDurationSecs))
      .map(message => EventLog.fromJson(message))
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Run Spark streaming
    run(eventDStream, params)
  }

  def run(events: DStream[EventLog], params: Params): Unit = {
    val ssc = events.context
    val projectId = params.projectId
    val namespace = params.datastoreNamespace
    val kind = params.datastoreKind

    // Create a update state spec in a stateful spark streaming
    val updateFunction = new UserActionStatUpdater(projectId, kind)
    val stateSpec = StateSpec.function(updateFunction.apply _).numPartitions(params.numPartitions)

    // Aggregate each user state from data in the current window
    val data = events
      .map(event => (event.userId, 1))
      .reduceByKey(_ + _)
      .map{ case (userId, total) => new UserActionStats(userId, total) }

    // Heatbeat
    data.count().print()

    // Gracefully stopping
    sys.ShutdownHookThread {
      log.info("Gracefully stopping Spark Streaming Application")
      ssc.stop(true, true)
      log.info("Application stopped")
    }

    // Start the spark streaming
    ssc.start
    ssc.awaitTermination
  }
}


object UserActionCounter {

  case class Params(
    projectId: String = "",
    inputTopic: String = "",
    datastoreNamespace: String = "",
    datastoreKind: String = "",
    checkpointPath: String = s"/tmp/spark-streaming-checkpoint-${getClass.getSimpleName}",
    numReceivers: Int = 2,
    numPartitions: Int = 20,
    windowDurationSecs: Int = 10)

  def main(args: Array[String]): Unit = {
    val parser = getOptionParser
    parser.parse(args, Params()) match {
      case Some(params) => {
        val conf = new SparkConf().setAppName(getClass.getSimpleName)
          .registerKryoClasses(Array(classOf[EventLog]))
        val sc = new SparkContext(conf)
        val app = new UserActionCounter()
        app.run(sc, params)
      }
      case None => {
        throw new IllegalArgumentException("params are invalid")
      }
    }
  }

  def getOptionParser: OptionParser[Params] = {
    new OptionParser[Params]("scopt") {
      opt[String]("projectId")
        .text("Google cloud project ID")
        .required()
        .action((x, c) => c.copy(projectId = x))
      opt[String]("checkpointPath")
        .text("Spark Streaming checkpoint path")
        .action((x, c) => c.copy(checkpointPath = x))
      opt[String]("inputTopic")
        .text("input Google Pub/Sub topic")
        .action((x, c) => c.copy(inputTopic = x))
      opt[String]("datastoreNamespace")
        .text("output Google Datastore namespace")
        .action((x, c) => c.copy(datastoreNamespace = x))
      opt[String]("datastoreKind")
        .text("output Google Datastore kind")
        .action((x, c) => c.copy(datastoreKind = x))
      opt[Int]("numReceivers")
        .text("the number of Pub/Sub receivers")
        .action((x, c) => c.copy(numReceivers = x))
      opt[Int]("numPartitions")
        .text("the number Spark Streaming partitions")
        .action((x, c) => c.copy(numPartitions = x))
      opt[Int]("windowDurationSecs")
        .text("Spark streaming window duration time")
        .action((x, c) => c.copy(windowDurationSecs = x))
    }
  }
}
