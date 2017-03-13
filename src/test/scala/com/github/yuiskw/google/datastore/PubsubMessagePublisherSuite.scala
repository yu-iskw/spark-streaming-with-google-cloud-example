package com.github.yuiskw.google.datastore

import com.github.yuiskw.google.datastore.PubsubMessagePublisher.Params
import org.scalatest.FunSuite

class PubsubMessagePublisherSuite extends FunSuite {

  test("test option parser") {
    val args = Array(
      "--projectId", "test-project-123",
      "--pubsubTopic", "test-topic"
    )
    val parser = PubsubMessagePublisher.getOptionParser
    val options = parser.parse(args, new Params())
    assert(options.get.projectId === "test-project-123")
    assert(options.get.pubsubTopic === "test-topic")
  }
}
