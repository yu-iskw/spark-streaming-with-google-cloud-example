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

import com.github.yuiskw.EventLog
import com.github.yuiskw.spark.streaming.UserActionCounter.Params
import com.google.cloud.datastore._
import scopt.OptionParser

import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


class UserActionState(val userId: Int, val numActions: Int) extends Serializable {

  def merge(another: UserActionState): UserActionState = {
    require(another.userId == this.userId)
    new UserActionState(userId, this.numActions + another.numActions)
  }
}


object UserActionState {

  val PROPERTY_NUM_ACTIONS = "num_actions"
  val PROPERTY_UPDATED = "updated"

  /**
    * Get or create an entity
    */
  def getOrCreate(
      datastore: Datastore,
      namespace: String,
      kind: String,
      state: UserActionState): Entity = {
    val key = getKey(datastore.getOptions.getProjectId, namespace, kind, state)
    // TODO replace `fetch` with `get`. `get` doesn't work in Scala.
    val entities = datastore.fetch(key)
    if (entities.size() == 1 && entities.get(0) != null) {
      entities.get(0)
    }
    else {
      // make a default user state
      Entity.newBuilder(key)
        .set(PROPERTY_NUM_ACTIONS, state.numActions)
        .set(PROPERTY_UPDATED, getCurrentDateTimeValue)
        .build()
    }
  }

  /**
    * Update an entity
    */
  def updateEntity(entity: Entity, state: UserActionState): Entity = {
    val actionsValue = LongValue.newBuilder(state.numActions).setExcludeFromIndexes(false).build()
    Entity.newBuilder(entity)
      .set(PROPERTY_NUM_ACTIONS, actionsValue)
      .set(PROPERTY_UPDATED, getCurrentDateTimeValue)
      .build()
  }

  /**
    * Get a Datastore key
    */
  def getKey(projectId: String, namespace: String, kind: String, state: UserActionState): Key = {
    new KeyFactory(projectId).setNamespace(namespace).setKind(kind).newKey(state.userId)
  }

  /**
    * Get a current time DateTimeValue
    */
  private def getCurrentDateTimeValue: DateTimeValue = DateTimeValue.of(DateTime.now())
}


/**
  * This class is used for updating Spark streaming state.
  *
  * @param projectId Google cloud project ID
  * @param outputKind Google Datastore Kind name
  */
class UserActionStatUpdater(val projectId: String, val outputKind: String) extends Serializable {

  /**
    * Update each user state
    *
    * @param userId user ID as a key
    * @param newState new value
    * @param state current values
    */
  def apply(
      userId: Int,
      newState: Option[UserActionState],
      state: State[UserActionState]): (Int, UserActionState) = {
    val updated = newState.getOrElse(new UserActionState(userId, 0))
      .merge(state.getOption().getOrElse(new UserActionState(userId, 0)))
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
      .map{ case (userId, sum) => (userId, new UserActionState(userId, sum)) }
      .mapWithState(stateSpec)
      .checkpoint(Seconds(params.windowDurationSecs))

    // Put state to Datastore
    val bcProjectId = events.context.sparkContext.broadcast(projectId)
    val bcNamespace = events.context.sparkContext.broadcast(namespace)
    val bcKind = events.context.sparkContext.broadcast(kind)
    data.foreachRDD { rdd =>
      rdd.foreachPartition { iter =>
        val datastore = DatastoreOptions.newBuilder()
          .setProjectId(bcProjectId.value).build().getService
        var batch = datastore.newBatch()
        iter.zipWithIndex.foreach { case ((userId, state), idx) =>
          val _projectId = bcProjectId.value
          val _namespace = bcNamespace.value
          val _kind = bcKind.value
          val key = UserActionState.getKey(_projectId, _namespace, _kind, state)
          var entity = UserActionState.getOrCreate(datastore, _namespace, _kind, state)
          entity = UserActionState.updateEntity(entity, state)

          // When the number of entities reaches the limit, put them at one time
          batch.put(entity)
          if ((idx + 1) % 500 == 0) {
            batch.submit()
            batch = datastore.newBatch()
          }
        }
        // Put rest entities
        if (batch.isActive) {
          batch.submit()
        }
      }
    }

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

  val DEFAULT_DATASTORE_NAMESPACE = "example"
  val DEFAULT_DATASTORE_KIND = "ExampleUserStats"
  val DEFAULT_CHECKPOINT_PATH = s"/tmp/checkpoint-${getClass.getSimpleName}"
  val DEFAULT_NUM_RECEIVERS = 4
  val DEFAULT_NUM_PARTITIONS = 20
  val DEFAULT_DURATION_SECS = 10

  /**
    * This class is used for the command line arguments parser
    */
  case class Params(
    projectId: String = "",
    inputTopic: String = "",
    datastoreNamespace: String = DEFAULT_DATASTORE_NAMESPACE,
    datastoreKind: String = DEFAULT_DATASTORE_KIND,
    checkpointPath: String = DEFAULT_CHECKPOINT_PATH,
    numReceivers: Int = DEFAULT_NUM_RECEIVERS,
    numPartitions: Int = DEFAULT_NUM_PARTITIONS,
    windowDurationSecs: Int = DEFAULT_DURATION_SECS)

  /**
    * Main class to submit a Spark Streaming job
    */
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

  /**
    * Get a command line artuments parser
    */
  def getOptionParser: OptionParser[Params] = {
    new OptionParser[Params]("scopt") {
      opt[String]("projectId")
        .text("Google cloud project ID")
        .required()
        .action((x, c) => c.copy(projectId = x))
      opt[String]("inputTopic")
        .text("input Google Pub/Sub topic")
        .required()
        .action((x, c) => c.copy(inputTopic = x))
      opt[String]("checkpointPath")
        .text("Spark Streaming checkpoint path")
        .action((x, c) => c.copy(checkpointPath = x))
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
