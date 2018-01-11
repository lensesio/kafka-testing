package com.landoop.kafka.testing

import java.net.{Socket, SocketException}
import java.util.Properties

import com.typesafe.scalalogging.StrictLogging
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel
import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.schemaregistry.rest.{SchemaRegistryConfig, SchemaRegistryRestApplication}
import io.confluent.kafka.schemaregistry.storage.{SchemaRegistry, SchemaRegistryIdentity}
import org.eclipse.jetty.server.Server

class SchemaRegistryService(val port: Int,
                            val zookeeperConnection: String,
                            val kafkaTopic: String,
                            val avroCompatibilityLevel: AvroCompatibilityLevel,
                            val masterEligibility: Boolean) extends StrictLogging {

  private val app = new SchemaRegistryRestApplication({
    val prop = new Properties
    prop.setProperty("port", port.asInstanceOf[Integer].toString)
    prop.setProperty(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zookeeperConnection)
    prop.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, kafkaTopic)
    prop.put(SchemaRegistryConfig.COMPATIBILITY_CONFIG, avroCompatibilityLevel.toString)
    prop.put(SchemaRegistryConfig.MASTER_ELIGIBILITY, masterEligibility.asInstanceOf[AnyRef])
    prop
  })

  val restServer = startServer(port)

  var Endpoint: String = getEndpoint(restServer)

  val restClient = new RestService(Endpoint)

  def startServer(port: Int, retries: Int = 5): Option[Server] = {
    var retry = retries > 0
    var restServer: Option[Server] = None
    if (retry) {
      if (isPortInUse(port)) {
        logger.info(s"Schema Registry Port $port is already in use")
        Thread.sleep(2000)
        startServer(port, retries - 1)
      } else {
        restServer = Some(app.createServer)
        restServer.get.start()
      }
    }
    restServer
  }

  def getEndpoint(restServer: Option[Server]): String = {
    if (restServer.isDefined) {
      val uri = restServer.get.getURI.toString
      if (uri.endsWith("/")) {
        uri.substring(0, uri.length - 1)
      } else {
        uri
      }
    } else ""
  }

  private def isPortInUse(port: Integer): Boolean = try {
    new Socket("127.0.0.1", port).close()
    true
  }
  catch {
    case e: SocketException => false
  }

  def close() {
    if (restServer.isDefined) {
      restServer.get.stop()
      restServer.get.join()
    }
  }

  def isMaster: Boolean = app.schemaRegistry.isMaster

  def setMaster(schemaRegistryIdentity: SchemaRegistryIdentity): Unit =
    app.schemaRegistry.setMaster(schemaRegistryIdentity)

  def myIdentity: SchemaRegistryIdentity = app.schemaRegistry.myIdentity

  def masterIdentity: SchemaRegistryIdentity = app.schemaRegistry.masterIdentity

  def schemaRegistry: SchemaRegistry = app.schemaRegistry
}
