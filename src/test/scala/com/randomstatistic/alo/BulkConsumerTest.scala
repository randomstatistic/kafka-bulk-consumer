package com.randomstatistic.alo

import org.scalatest._
import akka.testkit.{TestProbe, ImplicitSender, TestKitBase}
import akka.actor.{ActorRef, ActorSystem}
import kafka.consumer.{KafkaStream, ConsumerConfig, Consumer}
import kafka.serializer.StringDecoder
import kafka.producer.KeyedMessage
import java.util.UUID
import scala.concurrent.duration._
import scala.util.Random
import akka.actor.FSM.CurrentState
import com.randomstatistic.alo.BatchingActor.{MsgContent, AckableMessage, GetMessage, Serving}

class BulkConsumerTest extends FunSpec with TestKitBase with ShouldMatchers with BeforeAndAfterAll with ImplicitSender {

  implicit lazy val system = ActorSystem()
  lazy val kafka = new KafkaUtil()

  override def beforeAll() {
  }

  override def afterAll() {
    system.shutdown()
    kafka.shutdown()
  }


  describe("when consuming") {
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

    describe("can consume") {
      def msg = new KeyedMessage[String, Array[Byte]]("consuming", UUID.randomUUID().toString.getBytes)
      val maxInFlight = 4
      val quiescePeriod = 1.second
      val topicName = "bulk"
      val consumerTimeout = 100.millis
      val addConsumerCooldown = 1.millis

      lazy val consumerProps = kafka.createConsumerProperties(kafka.zkConnectString, "ignored", consumerTimeout.toMillis)
      lazy val producerProps = kafka.getProducerConfig(kafka.brokerStr)
      lazy val batchingConfig = BatchingActor.BatchingConfig(maxInFlight = maxInFlight, quiescePeriod = quiescePeriod)
      lazy val bulkConsumerConfig = BulkConsumerConfig(
        consumerProps = consumerProps,
        producerProps = producerProps,
        addConsumerCooldown = addConsumerCooldown,
        batchingConfig = batchingConfig)
      lazy val bulkConsumer = {
        kafka.createTopic(topicName)
        system.actorOf(BulkConsumer.props(topicName, "bulk-consumer-test", bulkConsumerConfig))
      }
      def produce() = {
        val key = Random.nextString(6)
        kafka.producer.send(new KeyedMessage[String,Array[Byte]](topicName, key, Random.nextString(6).getBytes))
        Thread.sleep(100)
        key
      }
      def checkReceive(probe: TestProbe, testNum: Int = 0, keyCheck: Option[String] = None) {
        keyCheck match {
          case Some(key) =>
            probe.expectMsgPF(consumerTimeout * 3, s"got message # $testNum with key $key") {
              case Some(AckableMessage(id, content)) if content.isInstanceOf[MsgContent] => content.asInstanceOf[MsgContent].key() should be(key)
            }
          case None =>
            probe.expectMsgPF(consumerTimeout * 3, s"got message # $testNum") {
              case Some(AckableMessage(id, content)) if content.isInstanceOf[MsgContent] => true // don't care what the message was
            }
        }
      }
      // Checks if the next message was a Some
      def checkSome(probe: TestProbe) = {
        probe.expectMsgPF(consumerTimeout * 3, s"got message") {
          case Some(_) => true
          case None => false
        }
      }
      def sendReceive(probe: TestProbe, times: Int) {
        for (i <- 0 until times) {
          val key = produce()
          bulkConsumer.tell(GetMessage, probe.ref)
          checkReceive(probe, i, Some(key))
        }
      }

      it("should consume and ack") {
        val probe = TestProbe()
        bulkConsumer.tell(GetMessage, probe.ref)
        probe.expectMsg(consumerTimeout * 3, None)

        // should be able to get maxInFlight messages without pause
        sendReceive(probe, maxInFlight)

        // we hit maxInFlight, and we only start with one batcher in the pool, so this request should get None
        bulkConsumer.tell(GetMessage, probe.ref)
        probe.expectMsg(quiescePeriod - 1.millis, None)

        // should've added a new batcher to the pool though, so wait for the first to become active again
        Thread.sleep(quiescePeriod.toMillis + 200)
        // we no longer know whether a given batcher gets a particular message, so produce a bunch
        (0 until maxInFlight * 50).foreach( _ => produce() )
        Thread.sleep(1000)
        // now we should be able to consume 2 * maxInFlight without pause
        val twoConsumerRunway = maxInFlight * 2
        val twoConsumerMessages = Range(0, twoConsumerRunway).map{ _ =>
          bulkConsumer.tell(GetMessage, probe.ref)
          checkSome(probe)
        }.count(identity)
        twoConsumerMessages should be(twoConsumerRunway +- 1)

        // should've added a third batcher, repeat
        Thread.sleep(quiescePeriod.toMillis + 200)
        // we no longer know whether a given batcher gets a particular message, so produce a bunch
        (0 until maxInFlight * 100).foreach( _ => produce() )
        Thread.sleep(1000)
        val threeConsumerRunway = maxInFlight * 3
        // now we should be able to consume 3 * maxInFlight without pause
        val threeConsumerMessages = Range(0, threeConsumerRunway).map{ _ =>
          bulkConsumer.tell(GetMessage, probe.ref)
          checkSome(probe)
        }.count(identity)
        threeConsumerMessages should be(threeConsumerRunway +- 2)

      }

    }
  }

}
