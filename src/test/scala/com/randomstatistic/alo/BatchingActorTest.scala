package com.randomstatistic.alo

import akka.testkit._
import akka.actor.{ActorSystem, FSM}
import org.scalatest.{ShouldMatchers, BeforeAndAfterAll, FunSpec}
import org.apache.zookeeper.server.{NIOServerCnxn, ZooKeeperServer}
import java.io.File
import java.net.{ServerSocket, InetSocketAddress}
import kafka.server.{KafkaConfig, KafkaServer}
import java.util.Properties
import scala.util.Random
import kafka.serializer.{DefaultDecoder, StringDecoder, StringEncoder}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.consumer._
import kafka.admin.AdminUtils
import kafka.producer.KeyedMessage
import com.randomstatistic.alo.BatchingActor.{AckableMessage, GetMessage, Serving}
import scala.concurrent.duration._

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
          createConsumerProperties(kafka.zkConnectString, kafka.testGroupId, "1"))
        )
        val stream: KafkaStream[String, String] =
          connector.createMessageStreams(Map(kafka.testTopic -> 1), new StringDecoder(), new StringDecoder()).apply(kafka.testTopic).head
        val msgIterator = stream.iterator()

        kafka.producer.send(new KeyedMessage[String,String](kafka.testTopic, "keystr", "valstr"))
        msgIterator.hasNext() should be(true)
        msgIterator.next().message() should be("valstr")
        msgIterator.hasNext() should be(false)
        connector.commitOffsets
        connector.shutdown()
      }
    }

    describe("when consuming") {
      it("should respond None when empty") {
        val consumerProps = createConsumerProperties(kafka.zkConnectString, "bogus", "2")
        val producerProps = getProducerConfig(kafka.brokerStr)
        val fsm = TestFSMRef(new BatchingActor(kafka.testTopic, "myGroup", consumerProps, producerProps))
        val ba: TestActorRef[BatchingActor] = fsm
        fsm.stateName should be(Serving)
        ba ! GetMessage
        expectMsg(100.millis, None)
      }
//      it("should respond with a message when there is one") {
//        val msg = new KeyedMessage[String, String](testTopic, "keystr", "valstr")
//        producer.send(msg)
//        Thread.sleep(100)
//        ba ! GetMessage
//        expectMsgClass(100.millis, AckableMessage.getClass)
//      }
    }

  }

  // A lot of the following was stolen from here:
  // https://github.com/apache/kafka/blob/0.8.1/core/src/test/scala/unit/kafka/utils/TestUtils.scala
  // because kafka doesn't publish a test artifact


  def IoTmpDir = System.getProperty("java.io.tmpdir")

  def tempDir(str: String = "batching-actor-test"): File = {
    val f = new File(IoTmpDir, str + "-" + Random.nextInt(1000000))
    f.mkdirs()
    f.deleteOnExit() // TODO: Doesn't seem to be working
    f
  }

  def availPort = {
    // seriously? this is the best practice?
    val sock = new ServerSocket(0)
    val port = sock.getLocalPort
    sock.close()
    port
  }

  def createBrokerConfig(nodeId: Int, zkConnect: String, port: Int = availPort): Properties = {
    val props = new Properties
    props.put("broker.id", nodeId.toString)
    props.put("host.name", "localhost")
    props.put("port", port.toString)
    props.put("log.dir", tempDir().getAbsolutePath)
    props.put("zookeeper.connect", zkConnect)
    props.put("replica.socket.timeout.ms", "1500")
    props.put("num.partitions", "100") // changes the default, used by auto-created topics
    props
  }

  def createConsumerProperties(zkConnect: String, groupId: String, consumerId: String,
                               consumerTimeout: Long = -1): Properties = {
    val props = new Properties
    props.put("zookeeper.connect", zkConnect)
    props.put("group.id", groupId)
    props.put("consumer.id", consumerId)
    props.put("consumer.timeout.ms", consumerTimeout.toString)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    //props.put("auto.commit.interval.ms", "1000")
    props.put("rebalance.max.retries", "4")
    props.put("auto.offset.reset", "smallest")
    props.put("num.consumer.fetchers", "2")

    props
  }

  def getProducerConfig(brokerList: String, partitioner: String = "kafka.producer.DefaultPartitioner"): Properties = {
    val props = new Properties()
    props.put("metadata.broker.list", brokerList)
    props.put("partitioner.class", partitioner)
    props.put("message.send.max.retries", "3")
    props.put("retry.backoff.ms", "1000")
    props.put("request.timeout.ms", "500")
    props.put("request.required.acks", "-1")  // all in-sync replicas
    props.put("serializer.class", classOf[StringEncoder].getName.toString)

    props
  }

  def waitUntilMetadataIsPropagated(servers: Seq[KafkaServer], topic: String, partition: Int, timeout: Long): Boolean = {
      waitUntilTrue(() =>
        servers.foldLeft(true)(_ && _.apis.metadataCache.containsTopicAndPartition(topic, partition)), timeout)
  }

  def waitUntilTrue(condition: () => Boolean, waitTime: Long): Boolean = {
    val startTime = System.currentTimeMillis()
    while (true) {
      if (condition())
        return true
      if (System.currentTimeMillis() > startTime + waitTime)
        return false
      println("Waiting for condition")
      Thread.sleep(waitTime.min(100L))
    }
    // should never hit here
    throw new RuntimeException("unexpected error")
  }


}
