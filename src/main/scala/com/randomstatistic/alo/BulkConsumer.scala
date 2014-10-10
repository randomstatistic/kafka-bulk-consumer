package com.randomstatistic.alo

import java.util.{UUID, Properties}
import akka.actor._
import java.util.regex.Pattern
import akka.actor.SupervisorStrategy.{Restart, Decider}
import akka.actor.OneForOneStrategy
import scala.collection.mutable
import scala.concurrent.duration._

import akka.actor.FSM.{Transition, SubscribeTransitionCallBack}



object BulkConsumer {

}

/**
 * This could probably be converted into an akka Router instance, but
 * Routers need to be thread-safe, so that gets more tricky.
 * @param topic
 * @param groupId
 * @tparam T
 */
class BulkConsumer[T](topic: String, groupId: String) extends Actor {
  import BatchingActor._

  case class MaskedUUIDGenerator(maskLen: Int) {
    require(maskLen <= 3) // let's not get out of control here, 3 means 3360 masks

    type IdGenerator = () => UUID
    def nextGenerator = {
      val nextMask = masksItr.next()
      (nextMask, maskedIdGenerator(nextMask))
    }

    def maskedIdGenerator(mask: String): IdGenerator = {
      require(mask.forall(c => c.toString.matches("[a-fA-F0-9]")))
      require(mask.length == maskLen)
      val re = "^" + "." * mask.length
      val reC = Pattern.compile(re)

      () => {
        val matcher = reC.matcher(UUID.randomUUID().toString)
        val masked = matcher.replaceFirst(mask)
        UUID.fromString(masked)
      }
    }

    def getMask(uuid: UUID) = {
      uuid.toString.substring(0, maskLen)
    }

    val masks: List[String] = {
      val charRange = (('0' to '9') ++ ('a' to 'f')).map(_.toString).toList
      val combinations =
        charRange.combinations(maskLen).map(_.permutations).flatten.map(_.mkString)
      combinations.toList
    }

    val masksItr = masks.toIterator

  }

  val decider: Decider = {
    case _: ActorInitializationException => Restart // kafka connection happens during initialization
  }
  override val supervisorStrategy = OneForOneStrategy()(decider.orElse(SupervisorStrategy.defaultDecider))

  val consumerProps = new Properties()
  val producerProps = new Properties()
  val baseConfig = new BatchingActor.BatchingConfig()
  val idGenerator = MaskedUUIDGenerator(1)

  def batchingActor(generator: () => UUID) =
    context.actorOf(BatchingActor.props(
      topic,
      groupId,
      consumerProps,
      producerProps,
      baseConfig.copy(idGenerator = generator)
    ).withDispatcher("bulk-consumer.pinned-dispatcher"))

  implicit val actorOrdering = Ordering.fromLessThan[ActorRef](_.hashCode() > _.hashCode()) // random, but deterministic
  val activeBatchers = mutable.TreeSet.empty[ActorRef]
  val inactiveBatchers = mutable.TreeSet.empty[ActorRef]
  val routes = mutable.Map.empty[String,ActorRef]
  
  val addConsumerCooldown = 10.seconds.toMillis
  var lastConsumerAddition = System.currentTimeMillis() - addConsumerCooldown
  tryAddConsumer // get the ball rolling

  def tryAddConsumer {
    if (System.currentTimeMillis() - lastConsumerAddition >= addConsumerCooldown) {
      val (mask, generator) = idGenerator.nextGenerator
      val newConsumer = batchingActor(generator)

      routes += Pair(mask, newConsumer)
      context.watch(newConsumer)
      newConsumer ! SubscribeTransitionCallBack(self)

      lastConsumerAddition = System.currentTimeMillis()
    }
  }
  def removeConsumer(ref: ActorRef) {
    activeBatchers.remove(ref)
    inactiveBatchers.remove(ref)
    val routeKeys = routes.filter{
      case (k, v) => ref == v
    }.map(_._1)
    routeKeys.foreach(routes.remove)
  }

  def markInactive(ref: ActorRef) {
    inactiveBatchers.add(ref)
    activeBatchers.remove(ref)
  }
  def markActive(ref: ActorRef) {
    activeBatchers.add(ref)
    inactiveBatchers.remove(ref)
  }

  override def receive: Receive = {
    case GetMessage =>
      if (activeBatchers.size > 0) {
        activeBatchers.head.forward(GetMessage)
      }
      else {
        if (routes.size < idGenerator.masks.length) tryAddConsumer
        sender ! None
      }
    case a: Acknowledgement =>
      routes.get(idGenerator.getMask(a.id)).map( routee => routee.forward(a) )

    case Transition(actorRef, BatchingActor.Serving, newState) => markInactive(actorRef)
    case Transition(actorRef, oldState, BatchingActor.Serving) => markActive(actorRef)  
    case Terminated(ref) => removeConsumer(ref)

  }
}



