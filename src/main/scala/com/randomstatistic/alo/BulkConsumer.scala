package com.randomstatistic.alo

import java.util.{UUID, Properties}
import akka.actor._
import java.util.regex.Pattern
import akka.actor.SupervisorStrategy.{Restart, Decider}
import akka.actor.OneForOneStrategy
import scala.collection.mutable
import scala.concurrent.duration._

import akka.actor.FSM.{Transition, SubscribeTransitionCallBack}
import com.randomstatistic.alo.BatchingActor.BatchingConfig


case class BulkConsumerConfig(
  idGenerator: MaskedUUIDGenerator = MaskedUUIDGenerator(1),
  consumerProps: Properties = new Properties,
  producerProps: Properties = new Properties,
  addConsumerCooldown: FiniteDuration = 30.seconds,
  batchingConfig: BatchingConfig = BatchingConfig()
)

object BulkConsumer {
  def defaultBatchingConfig = BulkConsumerConfig()

  def props(topic: String, groupId: String, config: BulkConsumerConfig = BulkConsumer.defaultBatchingConfig) =
    Props(new BulkConsumer(topic, groupId, config))
}

/**
 * This could probably be converted into an akka Router instance, but
 * Routers need to be thread-safe, so that gets more tricky.
 * @param topic
 * @param groupId
 */
class BulkConsumer(topic: String, groupId: String, config: BulkConsumerConfig = BulkConsumer.defaultBatchingConfig) extends Actor {
  import BatchingActor._


  val decider: Decider = {
    case _: ActorInitializationException => Restart // kafka connection happens during initialization
  }
  override val supervisorStrategy = OneForOneStrategy()(decider.orElse(SupervisorStrategy.defaultDecider))

  val consumerProps = config.consumerProps
  val producerProps = config.producerProps
  val idGenerator = config.idGenerator

  def batchingActor(generator: () => UUID) =
    context.actorOf(BatchingActor.props(
      topic,
      groupId,
      consumerProps,
      producerProps,
      config.batchingConfig.copy(idGenerator = generator)
    ).withDispatcher("bulk-consumer.pinned-dispatcher"))

  val batchers = mutable.Queue.empty[ActorRef]
  val routes = mutable.Map.empty[String,ActorRef]
  val servingStatus = mutable.Map.empty[ActorRef, Boolean].withDefaultValue(false)
  
  val addConsumerCooldown = config.addConsumerCooldown.toMillis
  var lastConsumerAddition = System.currentTimeMillis() - addConsumerCooldown


  def tryAddConsumer {
    if (System.currentTimeMillis() - lastConsumerAddition >= addConsumerCooldown && idGenerator.generators.hasNext) {
      val (mask, generator) = idGenerator.generators.next()
      val newConsumer = batchingActor(generator)

      routes += ((mask, newConsumer))
      batchers.enqueue(newConsumer)
      context.watch(newConsumer)
      newConsumer ! SubscribeTransitionCallBack(self)
      servingStatus(newConsumer) = true

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

  def markInactive(ref: ActorRef) {
    servingStatus(ref) = false
    if (servingStatus.forall( _._2 == false ))
      tryAddConsumer
    if (batchers.head == ref)
      batchers.enqueue(batchers.dequeue)
  }
  def markActive(ref: ActorRef)   = servingStatus(ref) = true

  def activeBatcher: Option[ActorRef] = {

    def findActive(stopOn: ActorRef): Option[ActorRef] = {
      if (batchers.head == stopOn) {
        // looped the batchers list without finding an active batcher
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

    if (batchers.isEmpty) {
      None
    }
    else if (servingStatus(batchers.head)) {
      Some(batchers.head) // use the front of the queue to get messages until we can't
    }
    else {
      // rotate the front of the queue to the back until the front is usable, or we do a complete cycle
      val checked = batchers.dequeue
      batchers.enqueue(checked)
      findActive(checked)
    }
  }

  override def preStart() = {
    tryAddConsumer
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
    case Transition(actorRef, oldState, newState) => Unit
    case Terminated(ref) => removeConsumer(ref)

  }
}



