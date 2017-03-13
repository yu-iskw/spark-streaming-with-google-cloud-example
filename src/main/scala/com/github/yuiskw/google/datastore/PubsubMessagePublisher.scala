package com.github.yuiskw.google.datastore

import com.github.yuiskw.EventLog
import com.google.cloud.pubsub.{Message, PubSubOptions}
import scopt.OptionParser

object PubsubMessagePublisher {

  /** This case class is used for the command line arguments parser */
  private[yuiskw] case class Params(projectId: String = "", pubsubTopic: String = "")

  def main(args: Array[String]): Unit = {
    val parser = getOptionParser
    parser.parse(args, Params()) match {
      case Some(params) => {
        val pubsubBuilder = PubSubOptions.newBuilder()
        pubsubBuilder.setProjectId(params.projectId)
        val pubsub = pubsubBuilder.build().getService()
        val topic = pubsub.getTopic(params.pubsubTopic)
        (1 to 1000).foreach { i =>
          topic.publish(Message.of(EventLog.getDummy(10).toJson))
        }
      }
      case None => {
        throw new IllegalArgumentException("params are invalid")
      }
    }
  }

  /**
    * Get a command line option parser
    */
  private[yuiskw]
  def getOptionParser: OptionParser[Params] = {
    new OptionParser[Params]("scopt") {
      opt[String]("projectId")
        .text("Google cloud project ID")
        .required()
        .action((x, c) => c.copy(projectId = x))
      opt[String]("pubsubTopic")
        .text("Google Pub/Sub topic")
        .required()
        .action((x, c) => c.copy(pubsubTopic = x))
    }
  }
}
