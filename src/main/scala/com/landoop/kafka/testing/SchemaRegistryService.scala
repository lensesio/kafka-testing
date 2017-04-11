package com.landoop.kafka.testing

import java.util.Properties

import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel
import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.schemaregistry.rest.{SchemaRegistryConfig, SchemaRegistryRestApplication}
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry
import io.confluent.kafka.schemaregistry.zookeeper.SchemaRegistryIdentity
import org.eclipse.jetty.server.Server

class SchemaRegistryService(val port: Int,
                            val zookeeperConnection: String,
                            val kafkaTopic: String,
                            val avroCompatibilityLevel: AvroCompatibilityLevel,
                            val masterEligibility: Boolean) {

  private val app = new SchemaRegistryRestApplication({
    val prop = new Properties
    prop.setProperty("port", port.asInstanceOf[Integer].toString)
    prop.setProperty(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zookeeperConnection)
    prop.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, kafkaTopic)
    prop.put(SchemaRegistryConfig.COMPATIBILITY_CONFIG, avroCompatibilityLevel.toString)
    prop.put(SchemaRegistryConfig.MASTER_ELIGIBILITY, masterEligibility.asInstanceOf[AnyRef])

    prop
  })

  val RestServer: Server = {
    val server = app.createServer
    server.start()
    server
  }

  val Endpoint: String = {
    val uri = RestServer.getURI.toString
    if (uri.endsWith("/")) uri.substring(0, uri.length - 1)
    else uri
  }

  val RestClient = new RestService(Endpoint)

  def close() {
    if (RestServer != null) {
      RestServer.stop()
      RestServer.join()
    }
  }

  def isMaster: Boolean = app.schemaRegistry.isMaster

  def setMaster(schemaRegistryIdentity: SchemaRegistryIdentity) {
    app.schemaRegistry.setMaster(schemaRegistryIdentity)
  }

  def myIdentity: SchemaRegistryIdentity = app.schemaRegistry.myIdentity

  def masterIdentity: SchemaRegistryIdentity = app.schemaRegistry.masterIdentity

  def schemaRegistry: SchemaRegistry = app.schemaRegistry
}
