package com.landoop.kafka.testing

import java.util.Properties

import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig
import kafka.admin.{AdminUtils, RackAwareMode}
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{CoreUtils, TestUtils, ZkUtils}
import kafka.zk.EmbeddedZookeeper
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.utils.SystemTime

import scala.collection.immutable.IndexedSeq

/**
  * Test harness to run against a real, local Kafka cluster and REST proxy. This is essentially
  * Kafka's ZookeeperTestHarness and KafkaServerTestHarness traits combined and ported to Java with
  * the addition of the REST proxy. Defaults to a 1-ZK, 3-broker, 1 REST proxy cluster.
  *
  * NOTE: the code makes sure the localhost, 0.0.0.0 are not going through the proxy
  */
sealed class KCluster(brokersNumber: Int = 1,
                      schemaRegistryEnabled: Boolean = true,
                      avroCompatibilityLevel: AvroCompatibilityLevel = AvroCompatibilityLevel.NONE) extends AutoCloseable {

  private val Zookeeper = new EmbeddedZookeeper
  val ZookeeperConnection = s"localhost:${Zookeeper.port}"

  var Connect: EmbeddedConnect = _
  var kafkaConnectEnabled: Boolean = false

  private val ZookeeperUtils = ZkUtils.apply(
    ZookeeperConnection,
    KCluster.ZKSessionTimeout,
    KCluster.ZKConnectionTimeout,
    setZkAcls()) // true or false doesn't matter because the schema registry Kafka principal is the same as the
  // Kafka broker principal, so ACLs won't make any difference. The principals are the same because
  // ZooKeeper, Kafka, and the Schema Registry are run in the same process during testing and hence share
  // the same JAAS configuration file. Read comments in ASLClusterTestHarness.java for more details.

  val ZKClient: ZkClient = ZookeeperUtils.zkClient

  val BrokersConfig: IndexedSeq[KafkaConfig] = (1 to brokersNumber).map(i => getKafkaConfig(i))

  val Brokers: IndexedSeq[KafkaServer] = BrokersConfig.map(TestUtils.createServer(_, new SystemTime()))

  //val BrokersPort: IndexedSeq[Int] = Brokers.map(_.boundPort(SecurityProtocol.PLAINTEXT))

  val BrokersList: String = TestUtils.getBrokerListStrFromServers(Brokers, getSecurityProtocol)

  val SchemaRegistryService: Option[SchemaRegistryService] = {
    if (schemaRegistryEnabled) {
      val schemaRegPort = PortProvider.one
      val scheamRegService = new SchemaRegistryService(schemaRegPort,
        ZookeeperConnection,
        KCluster.KAFKASTORE_TOPIC,
        avroCompatibilityLevel,
        true)

      Some(scheamRegService)
    } else {
      None
    }
  }


  private def setZkAcls() = {
    getSecurityProtocol == SecurityProtocol.SASL_PLAINTEXT ||
      getSecurityProtocol == SecurityProtocol.SASL_SSL
  }

  def createTopic(topicName: String, partitions: Int = 1, replication: Int = 1): Unit = {
    AdminUtils.createTopic(ZookeeperUtils, topicName, partitions, replication, new Properties, RackAwareMode.Enforced)
  }

  def startEmbeddedConnect(workerConfig: Properties, connectorConfigs: List[Properties]): Unit = {
    kafkaConnectEnabled = true
    Connect = EmbeddedConnect(workerConfig, connectorConfigs)
    Connect.start()
  }

  private def buildBrokers() = {
    (0 to brokersNumber).map(getKafkaConfig)
      .map { config =>
        val server = TestUtils.createServer(config, new SystemTime)
        (config, server)
      }.unzip
  }


  private def injectProperties(props: Properties): Unit = {
    props.setProperty("auto.create.topics.enable", "true")
    props.setProperty("num.partitions", "1")
    /*val folder = new File("kafka.cluster")
    if (!folder.exists())
      folder.mkdir()

    val logDir = new File("kafka.cluster", UUID.randomUUID().toString)
    logDir.mkdir()

    props.setProperty("log.dir", logDir.getAbsolutePath)*/
  }

  private def getKafkaConfig(brokerId: Int): KafkaConfig = {
    val props: Properties = TestUtils.createBrokerConfig(
      brokerId,
      ZookeeperConnection,
      enableControlledShutdown = false,
      enableDeleteTopic = false,
      TestUtils.RandomPort,
      interBrokerSecurityProtocol = None,
      trustStoreFile = None,
      KCluster.EMPTY_SASL_PROPERTIES,
      enablePlaintext = true,
      enableSaslPlaintext = false,
      TestUtils.RandomPort,
      enableSsl = false,
      TestUtils.RandomPort,
      enableSaslSsl = false,
      TestUtils.RandomPort,
      None)
    injectProperties(props)
    KafkaConfig.fromProps(props)
  }

  private def getSecurityProtocol = SecurityProtocol.PLAINTEXT


  def close(): Unit = {
    if (kafkaConnectEnabled) {
      Connect.stop()
    }
    SchemaRegistryService.foreach(_.close())
    Brokers.foreach { server =>
      server.shutdown
      CoreUtils.delete(server.config.logDirs)
    }

    ZookeeperUtils.close()
    Zookeeper.shutdown()
  }
}


object KCluster {
  val DEFAULT_NUM_BROKERS = 1
  val KAFKASTORE_TOPIC = SchemaRegistryConfig.DEFAULT_KAFKASTORE_TOPIC
  val EMPTY_SASL_PROPERTIES: Option[Properties] = None

  System.setProperty("http.nonProxyHosts", "localhost|0.0.0.0|127.0.0.1")

  // a larger connection timeout is required for SASL tests
  val ZKConnectionTimeout = 30000

  // SASL connections tend to take longer.
  val ZKSessionTimeout = 6000

}