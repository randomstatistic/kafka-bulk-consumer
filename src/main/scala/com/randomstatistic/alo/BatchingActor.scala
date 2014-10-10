package com.randomstatistic.alo

import akka.actor.{PoisonPill, Props, FSM, Actor}
import java.util.{UUID, Properties}
import kafka.consumer._
import kafka.producer.{ProducerConfig, Producer}
import kafka.serializer.{DefaultDecoder, StringDecoder}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import com.randomstatistic.alo.BatchingActor._
import scala.util.Success
import kafka.producer.KeyedMessage
import scala.util.Failure
import scala.Some
import scala.util.Success
import kafka.message.MessageAndMetadata

object BatchingActor {
  // FSM state management classes
  trait State
  case object Serving extends State
  case object Quiescing extends State
  case object Compacting extends State
  case object Committing extends State
  case object QuiesceComplete
  case object CompactComplete
  case object CommitComplete

  // collection of config options
  case class BatchingConfig(
    idGenerator: () => UUID = () => UUID.randomUUID(),
    maxInFlight: Int = 15,
    quiescePeriod: FiniteDuration = 1.second,
    maxTimeBetweenCommit: Duration = Duration.Inf,
    opportunisticCommitThreshold: Int = 8,
    replyAsParent: Boolean = false
  )

  // administrative & testing related messages
  case object GetInFlightCount
  case class InFlightCount(count: Int)

  // first-class API messages
  case object GetMessage
  trait Acknowledgement { val id: UUID }
  case class Ack(id: UUID) extends Acknowledgement
  case class Nack(id: UUID) extends Acknowledgement
  case class AckableMessage[T](id: UUID, msg: T)


  // Encapsulates the data associated with a given FSM state, including the kafka connection reference.
  // Otherwise, this just consists of tracking the outstanding messages awaiting acknowledgement, which
  // is stored in a map of id to:
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
    def unhandled = inFlight.filter{
      case (id, (msg, ackState)) => ackState.isEmpty || !ackState.get
    }.map {
      case ((id, (msg, ackState))) => msg
    }
  }

  // Kafka management
  type MsgContent = MessageAndMetadata[String, Array[Byte]]

  case class KafkaConnection(consumer: ConsumerConnector, stream: SlightlyBetterConsumerIterator[String, Array[Byte]], producer: Producer[String, Array[Byte]])

  def getConnection(topic: String, groupId: String, consumerProperties: Properties, producerProperties: Properties) = {
    consumerProperties.setProperty("group.id", groupId)  // TODO: Should we be overriding here?
    val connector: ConsumerConnector = Consumer.create(new ConsumerConfig(consumerProperties))
    val stream: KafkaStream[String, Array[Byte]] =
      connector.createMessageStreams(Map(topic -> 1), new StringDecoder(), new DefaultDecoder()).apply(topic).head
    val msgIterator = SlightlyBetterConsumerIterator(stream.iterator())

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

  def props(topic: String, groupId: String, consumerProps: Properties, producerProps: Properties, config: BatchingConfig = new BatchingConfig()) =
    Props(new BatchingActor(topic, groupId, consumerProps, producerProps, config))
}


class BatchingActor(topic: String, groupId: String, consumerProps: Properties, producerProps: Properties, config: BatchingConfig = new BatchingConfig()) extends Actor with FSM[BatchingActor.State, BatchingActor.QueueData[MsgContent]] {

  import BatchingActor._
  import context.dispatcher

  startWith(Serving, QueueData(getConnection(topic, groupId, consumerProps, producerProps), Map[UUID, (MsgContent, Option[Boolean])]()))

  whenUnhandled {
    // any state except Serving pretends there are no messages
    case Event(GetMessage, d) => stay replying None
    // multiple states accept acks
    case Event(Ack(id), d) => stay using d.ack(id)
    case Event(Nack(id), d) => stay using d.nack(id)
    case Event(GetInFlightCount, d) => stay replying InFlightCount(d.inFlight.size)
  }

  // TODO: Probably want some functional distinctions between these
  onTermination {
    case StopEvent(FSM.Normal, state, data)         => closeConnection(data.conn)
    case StopEvent(FSM.Shutdown, state, data)       => closeConnection(data.conn)
    case StopEvent(FSM.Failure(cause), state, data) => closeConnection(data.conn)
  }

  protected def nextMsg(d: QueueData[MsgContent]) = d.conn.stream.next()

  when(Serving) {
    case Event(GetMessage, d) => {
      val id = config.idGenerator()

      val msgOpt = if (d.conn.stream.hasNext)   // WARNING: can *block* for consumer.timeout.ms!
        Some(AckableMessage[MsgContent](id, nextMsg(d)))
      else
        None

      val newData = msgOpt match {
        case None => d
        case Some(msg) => d + msg
      }

      val newState: State =
        if (newData.inFlight.size < config.maxInFlight)
          stay
        else
          goto(Quiescing)

      if (config.replyAsParent)
        sender().tell(msgOpt, context.parent)
      else
        sender ! msgOpt

      // TODO: Duration-based state transition
      // TODO: Opportunistic state transition
      newState using newData
    }

  }

  onTransition {
    case x -> Quiescing => {
      setTimer("quiesce", QuiesceComplete, config.quiescePeriod)
    }
    case Quiescing -> x => {
      cancelTimer("quiesce")
    }
  }

  when(Quiescing, stateTimeout = 1.second) {
    case Event(QuiesceComplete, d) => goto(Compacting) using d
  }

  onTransition {
    case x -> Compacting => {
      setTimer("compact", StateTimeout, 1.second)
      val unhandled = nextStateData.unhandled
      val producer = nextStateData.conn.producer
      val requeueF = Future {
        unhandled.foreach(msg =>  // consider .par.foreach?
          producer.send(new KeyedMessage[String, Array[Byte]](topic, msg.key, msg.message))
        )
      }.onComplete{
        case Success(_) => self ! CompactComplete
        case Failure(e) =>
          //Couldn't requeue
          self ! PoisonPill
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
    case x -> Committing => {
      setTimer("commit", StateTimeout, 1.second)
      Future {
        nextStateData.conn.consumer.commitOffsets
      }.onComplete {
        case Success(_) => self ! CommitComplete
        case Failure(e) =>
          //Couldn't commit offsets
          self ! PoisonPill
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
