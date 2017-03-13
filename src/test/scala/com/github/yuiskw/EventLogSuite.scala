package com.github.yuiskw

import org.scalatest.FunSuite

class EventLogSuite extends FunSuite {

  test("generate dummy") {
    val eventLog = EventLog.getDummy()
    assert(eventLog.userId.isInstanceOf[Int])
    assert(EventLog.EVENT_TYPES.contains(eventLog.eventId))
    assert(eventLog.timestamp.isInstanceOf[Long])
  }

  test("convert to JSON string") {
    val eventLog = new EventLog(1, "view", 1490159332314L)
    val json = eventLog.toJson
    assert(json === "{\"userId\":1,\"eventId\":\"view\",\"timestamp\":1490159332314}")
  }
}
