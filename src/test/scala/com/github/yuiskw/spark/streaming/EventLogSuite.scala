package com.github.yuiskw.spark.streaming

import org.joda.time.DateTime
import org.scalatest.FunSuite

class EventLogSuite extends FunSuite {

  test("generate dummy") {
    val eventLog = EventLog.getDummy()
    assert(eventLog.userId.isInstanceOf[Int])
    assert(EventLog.EVENT_TYPES.contains(eventLog.eventId))
    assert(eventLog.ts.isInstanceOf[DateTime])
  }

  test("convert to JSON string") {
    val eventLog = new EventLog(1, "view")
    val json = eventLog.toJson
    assert(json === "{\"userId\":1,\"eventId\":\"view\",\"ts\":{}}")
  }
}
