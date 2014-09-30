package com.randomstatistic.alo

import kafka.consumer._
import kafka.serializer.DefaultDecoder
import kafka.serializer.StringDecoder
import java.util.Properties
import kafka.producer.{ProducerConfig, Producer}
import akka.actor.Actor
import akka.actor.Actor.Receive

class BulkConsumer[T](topic: String, groupId: String) extends Actor {
  import BatchingActor._

  val consumerProps = new Properties()

  //val kafkaPool = context.actorOf(BatchingActor.props(topic, groupId, consumerProps, producerProps, config))


  override def receive: Receive = {
    case GetMessage => ???
  }
}

object BulkConsumer {

}


