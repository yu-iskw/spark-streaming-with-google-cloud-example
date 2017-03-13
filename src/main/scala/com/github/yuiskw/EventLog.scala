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

package com.github.yuiskw

import scala.util.Random

import com.google.gson.GsonBuilder
import org.joda.time.{DateTime, DateTimeZone}


class EventLog(val userId: Int, val eventId: String, val timestamp: Long) extends Serializable {

  def this(userId: Int, eventId: String) =
    this(userId, eventId, DateTime.now(DateTimeZone.UTC).getMillis)

  def toJson: String = {
    val gson = new GsonBuilder().serializeNulls().create()
    gson.toJson(this)
  }
}

object EventLog {

  val EVENT_TYPES = Seq("view", "like", "comment")


  def fromJson(json: String): EventLog = {
    import org.json4s._
    import org.json4s.native.JsonMethods._
    implicit val formats = DefaultFormats
    parse(json).extract[EventLog]
  }

  def getDummy(maxUserId: Int = 10): EventLog = {
    val userId = new Random().nextInt(maxUserId) + 1
    val eventId = EVENT_TYPES.apply(new Random().nextInt(3))
    new EventLog(userId, eventId)
  }
}
