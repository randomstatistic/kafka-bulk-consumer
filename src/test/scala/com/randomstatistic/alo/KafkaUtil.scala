package com.randomstatistic.alo

import org.apache.zookeeper.server.{NIOServerCnxn, ZooKeeperServer}
import java.net.{ServerSocket, InetSocketAddress}
import kafka.server.{KafkaServer, KafkaConfig}
import kafka.producer.{ProducerConfig, Producer}
import java.util.Properties
import kafka.serializer.{DefaultEncoder, StringEncoder}
import java.io.File
import scala.util.Random
import kafka.admin.AdminUtils

class KafkaUtil {

  val tickTime = 2000
  val numConnections = 5000
  val zkClientPort = availPort

  val zkDirectory = tempDir("zookeeper")

  val zkServer = new ZooKeeperServer(zkDirectory, zkDirectory, tickTime)
  val zkServerFactory = new NIOServerCnxn.Factory(new InetSocketAddress(zkClientPort), numConnections)
  val zkConnectString = s"localhost:$zkClientPort"
  zkServerFactory.startup(zkServer)


  val brokerProps = new KafkaConfig(createBrokerConfig(1, zkConnectString))
  val broker = new KafkaServer(brokerProps)
  broker.startup()
  val brokerPort = broker.config.advertisedPort
  println("Broker port: " + brokerPort)
  val brokerStr = s"localhost:$brokerPort"

  val testTopic = "someTopic"
  val testGroupId = "group1"


  def createTopic(topic: String) = {
    AdminUtils.createTopic(broker.zkClient, topic, 10, 1)
    waitUntilMetadataIsPropagated(Seq(broker), topic, 1, 5000)
  }
  createTopic(testTopic)

  val producer = new Producer[String, Array[Byte]](new ProducerConfig(getProducerConfig(brokerStr)))

  def shutdown() {
    producer.close()
    broker.shutdown()
    zkServer.shutdown()
    zkServerFactory.shutdown()
  }

  // The following was stolen almost verbatim from here:
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

  def createConsumerProperties(zkConnect: String, groupId: String, consumerTimeout: Long = 100): Properties = {
    val props = new Properties
    props.put("zookeeper.connect", zkConnect)
    props.put("group.id", groupId)
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
    props.put("serializer.class", classOf[DefaultEncoder].getName)
    props.put("key.serializer.class", classOf[StringEncoder].getName)
    // TODO: producer.sync?
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
      //println("Waiting for condition")
      Thread.sleep(waitTime.min(100L))
    }
    // should never hit here
    throw new RuntimeException("unexpected error")
  }

}
