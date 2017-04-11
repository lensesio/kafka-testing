package com.landoop.kafka.testing

import java.lang.management.ManagementFactory
import java.net.{Socket, SocketException}
import java.rmi.registry.{LocateRegistry, Registry}
import java.rmi.server.UnicastRemoteObject
import java.util
import java.util.Properties
import javax.management.remote.{JMXConnectorServer, JMXConnectorServerFactory, JMXServiceURL}

import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

trait ClusterTestingCapabilities extends WordSpec with Matchers with BeforeAndAfterAll {

  System.setProperty("http.nonProxyHosts", "localhost|0.0.0.0|127.0.0.1")

  val SCHEMA_REGISTRY_URL = "schema.registry.url"

  var registry: Registry = _
  val kafkaCluster: KCluster = new KCluster()

  var jmxConnectorServer: Option[JMXConnectorServer] = None

  /**
    * Run this method to enable JMX statistics across all embedded apps
    *
    * @param port - The JMX port to enable RMI stats
    */
  def loadJMXAgent(port: Int, retries: Int = 5): Unit = {
    var retry = retries > 0
    if (retry) {
      if (isPortInUse(port)) {
        println(s"JMX Port $port already in use")
        Thread.sleep(2000)
        loadJMXAgent(port, retries - 1)
      } else {
        println(s"Starting JMX Port of embedded Kafka system $port")
        val mbeanServer = ManagementFactory.getPlatformMBeanServer
        registry = LocateRegistry.createRegistry(port)
        val env = mutable.Map[String, String]()
        env += ("com.sun.management.jmxremote.authenticate" -> "false")
        env += ("com.sun.management.jmxremote.ssl" -> "false")
        val jmxServiceURL = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://:" + port + "/jmxrmi")
        jmxConnectorServer = Some(JMXConnectorServerFactory.newJMXConnectorServer(jmxServiceURL, env.asJava, mbeanServer))
        jmxConnectorServer.get.start()
        retry = false
        Thread.sleep(2000)
      }
    }
  }

  def startEmbeddedConnect(workerConfig: Properties, connectorConfigs: List[Properties]): Unit = {
    kafkaCluster.startEmbeddedConnect(workerConfig, connectorConfigs)
  }

  def isPortInUse(port: Integer): Boolean = {
    try {
      new Socket("127.0.0.1", port).close()
      true
    }
    catch {
      case e: SocketException => false
    }
  }

  protected override def afterAll(): Unit = {
    println("Cleaning embedded cluster")
    if (jmxConnectorServer.isDefined) {
      jmxConnectorServer.get.stop()
      registry.list().foreach(registry.unbind)
      UnicastRemoteObject.unexportObject(registry, true)
    }
    try {
      kafkaCluster.close()
    } catch {
      case e:Throwable => // ignore
    }
  }

  /** Helpful Producers **/
  def avroAvroProducerProps: Properties = {
    val props = new Properties
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.BrokersList)
    props.put(SCHEMA_REGISTRY_URL, kafkaCluster.SchemaRegistryService.get.Endpoint)
    props
  }

  def getAvroProducerProps[T <: Serializer[_]](ser: Class[T]): Properties = {
    val props = new Properties
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ser)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.BrokersList)
    props.put(SCHEMA_REGISTRY_URL, kafkaCluster.SchemaRegistryService.get.Endpoint)
    props
  }

  def stringAvroProducerProps: Properties = {
    val props = new Properties
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer])
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.BrokersList)
    props.put(SCHEMA_REGISTRY_URL, kafkaCluster.SchemaRegistryService.get.Endpoint)
    props
  }

  def stringstringProducerProps: Properties = {
    val props = new Properties
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.BrokersList)
    props.put(SCHEMA_REGISTRY_URL, kafkaCluster.SchemaRegistryService.get.Endpoint)
    props
  }

  def createProducer[K, T](props: Properties): KafkaProducer[K, T] = new KafkaProducer[K, T](props)

  /** Helpful Consumers **/
  def stringAvroConsumerProps(group: String = "stringAvroGroup"): Properties = {
    val props = new Properties
    props.put("bootstrap.servers", kafkaCluster.BrokersList)
    props.put("group.id", group)
    props.put("session.timeout.ms", "6000") // default value of group.min.session.timeout.ms.
    props.put("heartbeat.interval.ms", "2000")
    props.put("auto.commit.interval.ms", "1000")
    props.put("auto.offset.reset", "earliest")
    props.put("key.deserializer", classOf[StringDeserializer])
    props.put("value.deserializer", classOf[KafkaAvroDeserializer])
    props.put(SCHEMA_REGISTRY_URL, kafkaCluster.SchemaRegistryService.get.Endpoint)
    props
  }

  def stringstringConsumerProps(group: String = "stringstringGroup"): Properties = {
    val props = new Properties
    props.put("bootstrap.servers", kafkaCluster.BrokersList)
    props.put("group.id", group)
    props.put("session.timeout.ms", "6000") // default value of group.min.session.timeout.ms.
    props.put("heartbeat.interval.ms", "2000")
    props.put("auto.commit.interval.ms", "1000")
    props.put("auto.offset.reset", "earliest")
    props.put("key.deserializer", classOf[StringDeserializer])
    props.put("value.deserializer", classOf[StringDeserializer])
    props
  }

  def bytesbytesConsumerProps(group: String = "bytes2bytesGroup"): Properties = {
    val props = new Properties
    props.put("bootstrap.servers", kafkaCluster.BrokersList)
    props.put("group.id", group)
    props.put("session.timeout.ms", "6000") // default value of group.min.session.timeout.ms.
    props.put("heartbeat.interval.ms", "2000")
    props.put("auto.commit.interval.ms", "1000")
    props.put("auto.offset.reset", "earliest")
    props.put("key.deserializer", classOf[BytesDeserializer])
    props.put("value.deserializer", classOf[BytesDeserializer])
    props
  }

  def createStringAvroConsumer(props: Properties): KafkaConsumer[String, AnyRef] = {
    new KafkaConsumer[String, AnyRef](props)
  }

  /** Consume **/
  def consumeStringAvro(consumer: Consumer[String, AnyRef], topic: String, numMessages: Int): Seq[AnyRef] = {

    consumer.subscribe(util.Arrays.asList(topic))

    def accum(records: Seq[AnyRef]): Seq[AnyRef] = {
      if (records.size < numMessages) {
        val consumedRecords = consumer.poll(1000)
        accum(consumedRecords.foldLeft(records) { case (acc, r) =>
          acc :+ r.value()
        })
      } else {
        consumer.close()
        records
      }
    }

    accum(Vector.empty)
  }

  def consumeRecords[K, V](consumer: Consumer[K, V], topic: String): Iterator[ConsumerRecord[K, V]] = {
    consumer.subscribe(util.Arrays.asList(topic))
    val result = Iterator.continually {
      consumer.poll(1000)
    }.flatten
    result
  }

}
