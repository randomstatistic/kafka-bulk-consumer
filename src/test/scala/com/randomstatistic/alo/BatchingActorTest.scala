package com.randomstatistic.alo

import akka.testkit._
import akka.actor.{ActorSystem, FSM}
import org.scalatest.{ShouldMatchers, BeforeAndAfterAll, FunSpec}
import org.apache.zookeeper.server.{NIOServerCnxn, ZooKeeperServer}
import java.io.File
import java.net.{ServerSocket, InetSocketAddress}
import kafka.server.{KafkaConfig, KafkaServer}
import java.util.{UUID, Properties}
import scala.util.Random
import kafka.serializer.{DefaultDecoder, StringDecoder, StringEncoder}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.consumer._
import kafka.admin.AdminUtils
import kafka.producer.KeyedMessage
import com.randomstatistic.alo.BatchingActor._
import scala.concurrent.duration._
import akka.actor.FSM.{Transition, CurrentState, SubscribeTransitionCallBack}
import com.randomstatistic.alo.BatchingActor.InFlightCount
import kafka.producer.KeyedMessage
import akka.actor.FSM.Transition
import akka.actor.FSM.CurrentState
import scala.Some
import com.randomstatistic.alo.BatchingActor.AckableMessage
import akka.actor.FSM.SubscribeTransitionCallBack

class BatchingActorTest extends FunSpec with TestKitBase with ShouldMatchers with BeforeAndAfterAll with ImplicitSender {

  implicit lazy val system = ActorSystem()
  lazy val kafka = new KafkaUtil()

  override def beforeAll {
  }

  override def afterAll {
    system.shutdown()
    kafka.shutdown()
  }

  describe("when batching") {

    describe("can start the test harness") {
      it("should have started zk") {
        kafka.zkServerFactory.getZooKeeperServer.isRunning should equal(true)
      }
      it("should produce and consume") {
        val connector = Consumer.create(new ConsumerConfig(
          kafka.createConsumerProperties(kafka.zkConnectString, kafka.testGroupId))
        )
        val stream: KafkaStream[String, String] =
          connector.createMessageStreams(Map(kafka.testTopic -> 1), new StringDecoder(), new StringDecoder()).apply(kafka.testTopic).head
        val msgIterator = SlightlyBetterConsumerIterator(stream.iterator())

        kafka.producer.send(new KeyedMessage[String,Array[Byte]](kafka.testTopic, "keystr", "valstr".getBytes))
        Thread.sleep(100)
        msgIterator.hasNext should be(true)
        msgIterator.next().message() should be("valstr")
        msgIterator.hasNext should be(false)
        connector.commitOffsets
        connector.shutdown()
      }
    }

    describe("when consuming") {
      def msg = new KeyedMessage[String, Array[Byte]]("consuming", UUID.randomUUID().toString.getBytes)
      val maxInFlight = 4
      val quiescePeriod = 100.millis
      lazy val consumerProps = kafka.createConsumerProperties(kafka.zkConnectString, "ignored")
      lazy val producerProps = kafka.getProducerConfig(kafka.brokerStr)
      lazy val batchingConfig = BatchingActor.BatchingConfig(maxInFlight = maxInFlight, quiescePeriod = quiescePeriod)
      lazy val fsm = {
        kafka.createTopic("consuming")
        TestFSMRef(new BatchingActor("consuming", kafka.testGroupId, consumerProps, producerProps, batchingConfig))
      }
      lazy val ba: TestActorRef[BatchingActor] = fsm

      it("should respond None when empty") {
        fsm.stateName should be(Serving)
        ba ! GetMessage
        expectMsg(100.millis, None)
      }
      it("should respond with a message when there is one") {
        fsm.stateName should be(Serving)
        kafka.producer.send(msg)
        Thread.sleep(100)
        ba ! GetMessage
        expectMsgPF(100.millis, "got the message") {
          case Some(AckableMessage(_, a)) => a
        }
      }
      it("should transition due to maxInFlight") {
        fsm.stateName should be(Serving)
        // watch the transitions
        val probe = TestProbe()
        ba ! SubscribeTransitionCallBack(probe.ref)
        probe.expectMsgPF(100.millis, "got the current state") {
          case CurrentState(ref, Serving) => ref
        }

        // figure out how many more messages are needed to trigger a transition
        ba ! GetInFlightCount
        val inFlight = expectMsgPF(100.millis, "got the count") {
          case InFlightCount(count) => count
        }
        val transitionsAfter = maxInFlight - inFlight
        transitionsAfter should be > 1

        // trigger a Serving -> Quiescing
        Range(0,transitionsAfter).foreach(x => kafka.producer.send(msg))
        Thread.sleep(100)
        val newMessages = Range(0,transitionsAfter).map(x => {
          ba ! GetMessage
          expectMsgPF(100.millis, "got the message") {
            case Some(a: AckableMessage[BatchingActor.MsgContent]@unchecked) => a
          }
        }).toSet
        val queueSize = inFlight + transitionsAfter
        ba ! Ack(newMessages.head.id) // positively acknowledge the first one, leave the rest

        probe.expectMsgPF(100.millis, "got the transition to Quiescing") {
          case Transition(ref, Serving, Quiescing) => ref
        }

        // wait for a Quiescing -> Compacting
        probe.expectMsgPF(200.millis, "got the transition to Compacting") {
          case Transition(ref, Quiescing, Compacting) => ref
        }

        // wait for a Compacting -> Committing
        probe.expectMsgPF(1.second, "got the transition to Committing") {
          case Transition(ref, Compacting, Committing) => ref
        }

        // wait for a Committing -> Serving
        probe.expectMsgPF(1.second, "got the transition to Serving again") {
          case Transition(ref, Committing, Serving) => ref
        }

        // messages that weren't acked should show up again
        var unseen = newMessages.tail.map(x => new String(x.msg.message()))
        //println("looking for " + unseen)
        Range(0,queueSize - 1).foreach( x => {   // we acked 1 of them
          ba ! GetMessage
          expectMsgPF(100.millis, "got the message") {
            case Some(AckableMessage(id, a: BatchingActor.MsgContent)) => {
              val asStr = new String(a.message())
              if (unseen.contains(asStr)) {
                unseen = unseen - asStr
              }
              //println("Got " + asStr)
            }
          }
        })
        unseen.size should be(0)

      }
    }

  }

}
