package com.randomstatistic.alo

import akka.actor.{Props, FSM, Actor}
import java.util.{UUID, Properties}
import kafka.consumer._
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import kafka.serializer.{DefaultDecoder, StringDecoder}
import scala.collection.mutable
import com.sun.tools.javac.comp.Todo
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import com.randomstatistic.alo.BatchingActor.{AckableMessage, GetMessage, QueueData, Serving}
import scala.util.{Failure, Success}

object BatchingActor {
  trait State
  case object Serving extends State
  case object Quiescing extends State
  case object Compacting extends State
  case object Committing extends State

  case object QuiesceComplete
  case object CompactComplete
  case object CommitComplete


  // map id to:
  //  case None => no ack/nack received
  //  case Some(true) => acked
  //  case Some(false) => nacked
  case class QueueData[T](conn: KafkaConnection, inFlight: Map[UUID, (T, Option[Boolean])]) {
    def +(pair: (UUID, (T, Option[Boolean]))) = this.copy(inFlight = inFlight + pair)
    def +(msg: AckableMessage[T]) = this.copy(inFlight = inFlight + (msg.id -> (msg.msg, None)))
    private def updateIfExists(id: UUID, value: Option[Boolean]) = {
      val current = inFlight.get(id)
      this.copy(inFlight = current match {
        case None => inFlight
        case Some((msg, _)) => inFlight.updated(id, (msg, value))
      })
    }
    def ack(id: UUID) = updateIfExists(id, Some(true))
    def nack(id: UUID) = updateIfExists(id, Some(false))
    def clear = this.copy(inFlight = inFlight.empty)
    def unhandled = inFlight.filter{ case (id, (msg, ackState)) => ackState.isEmpty || !ackState.get }
  }

  case object GetMessage
  trait Acknowledgement { val id: UUID }
  case class Ack(id: UUID) extends Acknowledgement
  case class Nack(id: UUID) extends Acknowledgement

  case class AckableMessage[T](id: UUID, msg: T)

  case class KafkaConnection(consumer: ConsumerConnector, stream: ConsumerIterator[String, Array[Byte]], producer: Producer[String, Array[Byte]])

  def getConnection(topic: String, groupId: String, consumerProperties: Properties, producerProperties: Properties) = {
    consumerProperties.setProperty("group.id", groupId)  // TODO: Should we be overriding here?
    val connector: ConsumerConnector = Consumer.create(new ConsumerConfig(consumerProperties))
    val stream: KafkaStream[String, Array[Byte]] =
      connector.createMessageStreams(Map(topic -> 1), new StringDecoder(), new DefaultDecoder()).apply(topic).head
    val msgIterator = stream.iterator()

    val connection = KafkaConnection(
      connector,
      msgIterator,
      new Producer[String, Array[Byte]](new ProducerConfig(producerProperties))
    )

    connection
  }
  def closeConnection(conn: KafkaConnection) {
    conn.consumer.shutdown()
    conn.producer.close()
  }

  def apply(topic: String, groupId: String, consumerProps: Properties, producerProps: Properties) =
    Props(new BatchingActor(topic, groupId, consumerProps, producerProps))
}


class BatchingActor(topic: String, groupId: String, consumerProps: Properties, producerProps: Properties) extends Actor with FSM[BatchingActor.State, BatchingActor.QueueData[Array[Byte]]] {
  type MsgContent = Array[Byte] // If you change this, change the FSM mixin type too
  import BatchingActor._
  import context.dispatcher

  val maxSize = 15

  startWith(Serving, QueueData(getConnection("test topic", "groupId", consumerProps, producerProps), Map[UUID, (MsgContent, Option[Boolean])]()))

  whenUnhandled {
    // any state except Serving pretends there are no messages
    case Event(GetMessage, d) => stay replying None
    // multiple states accept acks
    case Event(Ack(id), d) => stay using d.ack(id)
    case Event(Nack(id), d) => stay using d.nack(id)
  }

  // TODO: Probably want some functional distinctions between these
  onTermination {
    case StopEvent(FSM.Normal, state, data)         => closeConnection(data.conn)
    case StopEvent(FSM.Shutdown, state, data)       => closeConnection(data.conn)
    case StopEvent(FSM.Failure(cause), state, data) => closeConnection(data.conn)
  }

  when(Serving) {
    case Event(GetMessage, d) => {
      val id = UUID.randomUUID()
      
      val msgOpt = if (d.conn.stream.hasNext())
        Some(AckableMessage[MsgContent](id, d.conn.stream.next().message()))
      else
        None

      val newData = msgOpt match {
        case None => d
        case Some(msg) => d + msg
      }

      val newState = if (d.inFlight.size < maxSize)
        stay
      else
        goto(Quiescing)
      
      newState using newData replying msgOpt
    }

  }

  onTransition {
    case Serving -> Quiescing => {
      setTimer("quiesce", QuiesceComplete, 1.second)
    }
    case Quiescing -> x => {
      cancelTimer("quiesce")
    }
  }

  when(Quiescing, stateTimeout = 1.second) {
    case Event(QuiesceComplete, d) => goto(Compacting) using d
  }

  onTransition {
    case Quiescing -> Compacting => {
      setTimer("compact", StateTimeout, 1.second)
      val unhandled = nextStateData.unhandled
      val producer = nextStateData.conn.producer
      val requeueF = Future {
        unhandled.values.foreach {   // consider .par.foreach?
          // TODO: I copied this from elsewhere, but is "topic" really the right key?
          //case (msg, _) => producer.send(new KeyedMessage[String, MsgContent](topic, msg))
          // Trying "null" instead, which with the DefaultPartinitioner means "random partition"
          case (msg, _) => producer.send(new KeyedMessage[String, MsgContent](null, msg))
        }
      }.onComplete{
        case Success(_) => self ! CompactComplete
        case Failure(e) => throw new RuntimeException("Couldn't requeue", e)
      }
    }
    case Compacting -> x => {
      cancelTimer("compact")
    }
  }


  when(Compacting) {
    case Event(CompactComplete, d) => goto(Committing)
    case Event(StateTimeout, d) => throw new RuntimeException("Timed out trying to compact")
    // ignore acks, it's too late
    case Event(Ack(id), d) => stay
    case Event(Nack(id), d) => stay

  }

  onTransition {
    case Compacting -> Committing => {
      setTimer("commit", StateTimeout, 1.second)
      Future {
        nextStateData.conn.consumer.commitOffsets
      }.onComplete {
        case Success(_) => self ! CommitComplete
        case Failure(e) => throw new RuntimeException("Couldn't commit offsets", e)
      }
    }
    case Committing -> x => {
      cancelTimer("commit")
    }
  }

  when(Committing) {
    case Event(CommitComplete, d) => goto(Serving) using d.clear
    case Event(StateTimeout, d) => throw new RuntimeException("Timed out trying to commit")
    // ignore acks, it's too late
    case Event(Ack(id), d) => stay
    case Event(Nack(id), d) => stay
  }


}
