package com.randomstatistic.alo

import java.util.{UUID, Properties}
import akka.actor._
import java.util.regex.Pattern
import akka.actor.SupervisorStrategy.{Restart, Decider}
import akka.actor.OneForOneStrategy
import scala.collection.mutable
import scala.concurrent.duration._

import akka.actor.FSM.{Transition, SubscribeTransitionCallBack}


case class BulkConsumerConfig(
  idGenerator: MaskedUUIDGenerator = MaskedUUIDGenerator(1),
  consumerProps: Properties = new Properties,
  producerProps: Properties = new Properties
)

object BulkConsumer {
  def defaultBatchingConfig = BulkConsumerConfig()

}

/**
 * This could probably be converted into an akka Router instance, but
 * Routers need to be thread-safe, so that gets more tricky.
 * @param topic
 * @param groupId
 * @tparam T
 */
class BulkConsumer[T](topic: String, groupId: String, config: BulkConsumerConfig = BulkConsumer.defaultBatchingConfig) extends Actor {
  import BatchingActor._


  val decider: Decider = {
    case _: ActorInitializationException => Restart // kafka connection happens during initialization
  }
  override val supervisorStrategy = OneForOneStrategy()(decider.orElse(SupervisorStrategy.defaultDecider))

  val consumerProps = config.consumerProps
  val producerProps = config.producerProps
  val idGenerator = MaskedUUIDGenerator(1)

  def batchingActor(generator: () => UUID) =
    context.actorOf(BatchingActor.props(
      topic,
      groupId,
      consumerProps,
      producerProps,
      new BatchingConfig(idGenerator = generator)
    ).withDispatcher("bulk-consumer.pinned-dispatcher"))

  val batchers = mutable.Queue.empty[ActorRef]
  val routes = mutable.Map.empty[String,ActorRef]
  val servingStatus = mutable.Map.empty[ActorRef, Boolean].withDefaultValue(false)
  
  val addConsumerCooldown = 10.seconds.toMillis
  var lastConsumerAddition = System.currentTimeMillis() - addConsumerCooldown
  tryAddConsumer // get the ball rolling

  def tryAddConsumer {
    if (System.currentTimeMillis() - lastConsumerAddition >= addConsumerCooldown && idGenerator.generators.hasNext) {
      val (mask, generator) = idGenerator.generators.next()
      val newConsumer = batchingActor(generator)

      routes += ((mask, newConsumer))
      batchers.enqueue(newConsumer)
      context.watch(newConsumer)
      newConsumer ! SubscribeTransitionCallBack(self)

      lastConsumerAddition = System.currentTimeMillis()
    }
  }
  def removeConsumer(ref: ActorRef) {
    batchers.dequeueFirst(_ == ref)
    val routeKeys = routes.filter{
      case (k, v) => ref == v
    }.map(_._1)
    routeKeys.foreach(routes.remove)
  }

  def markInactive(ref: ActorRef) = servingStatus(ref) = false
  def markActive(ref: ActorRef)   = servingStatus(ref) = true

  def activeBatcher: Option[ActorRef] = {

    def findActive(stopOn: ActorRef): Option[ActorRef] = {
      if (batchers.head == stopOn) {
        None
      }
      else if (servingStatus(batchers.head)) {
        Some(batchers.head)
      }
      else {
        batchers.enqueue(batchers.dequeue)
        findActive(stopOn)
      }
    }

    if (servingStatus(batchers.head)) {
      Some(batchers.head)
    }
    else {
      val stopOn = batchers.dequeue
      batchers.enqueue(stopOn)
      findActive(stopOn)
    }
  }

  override def receive: Receive = {
    case GetMessage =>
      activeBatcher match {
        case Some(batcher) =>
          batcher.forward(GetMessage)
        case None =>
          tryAddConsumer
          sender ! None
      }

    case a: Acknowledgement =>
      routes.get(idGenerator.getMask(a.id)).map( routee => routee.forward(a) )

    case Transition(actorRef, BatchingActor.Serving, newState) => markInactive(actorRef)
    case Transition(actorRef, oldState, BatchingActor.Serving) => markActive(actorRef)  
    case Terminated(ref) => removeConsumer(ref)

  }
}



