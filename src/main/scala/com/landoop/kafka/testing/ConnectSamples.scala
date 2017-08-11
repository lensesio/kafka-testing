package com.landoop.kafka.testing

import java.util
import java.util.Properties

import org.apache.kafka.connect.runtime.distributed.DistributedConfig
import org.apache.kafka.connect.runtime.{ConnectorConfig, WorkerConfig}

import scala.collection.JavaConverters._

object ConnectSamples {

  def workerConfig(bootstapServers: String, schemaRegistryUrl: String): util.Map[String, AnyRef] = Map(
    DistributedConfig.GROUP_ID_CONFIG -> "testing-group-id",
    WorkerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstapServers,
    WorkerConfig.KEY_CONVERTER_CLASS_CONFIG -> "org.apache.kafka.connect.json.JsonConverter",
    WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG -> "org.apache.kafka.connect.json.JsonConverter",
    WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG -> "com.qubole.streamx.ByteArrayConverter",

    DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG -> "connect-offsets",
    DistributedConfig.CONFIG_TOPIC_CONFIG -> "connect-configs",
    DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG -> "connect-status",
    WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG -> "org.apache.kafka.connect.json.JsonConverter",
    WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG -> "org.apache.kafka.connect.json.JsonConverter",
    "schema.registry.url" -> schemaRegistryUrl
  ).asInstanceOf[Map[String, AnyRef]].asJava

  val sourceConfig: util.Map[String, AnyRef] = Map(
    ConnectorConfig.NAME_CONFIG -> "file-source-connector",
    ConnectorConfig.CONNECTOR_CLASS_CONFIG -> "org.apache.kafka.connect.file.FileStreamSourceConnector",
    ConnectorConfig.TASKS_MAX_CONFIG -> "1",
    "topic" -> "file-topic",
    "file" -> "/var/log/*"
  ).asInstanceOf[Map[String, AnyRef]].asJava

  def workerProperties(bootstapServers: String, schemaRegistryUrl: String): Properties = {
    val props = new Properties()
    props.putAll(workerConfig(bootstapServers, schemaRegistryUrl))
    props
  }

  val sourceProperties: Properties = {
    val props = new Properties()
    props.putAll(sourceConfig)
    props
  }

}
