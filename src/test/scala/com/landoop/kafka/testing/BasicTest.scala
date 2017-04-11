package com.landoop.kafka.testing

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.ProducerRecord

class BasicTest extends ClusterTestingCapabilities {

  private def createAvroRecord = {
    val userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", " + "\"name\": \"User\"," + "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}]}"
    val parser = new Schema.Parser
    val schema = parser.parse(userSchema)
    val avroRecord = new GenericData.Record(schema)
    avroRecord.put("name", "testUser")
    avroRecord
  }

  "KCluster" should {
    "start up and be able to handle avro records being sent " in {
      val topic = "testAvro"
      val avroRecord = createAvroRecord
      val objects = Array[AnyRef](avroRecord)
      val producerProps = stringAvroProducerProps
      val producer = createProducer[String,Any](producerProps)

      for (o <- objects) {
        val message = new ProducerRecord[String, Any](topic, o)
        producer.send(message)
      }
      val consumerProps = stringAvroConsumerProps()
      val consumer = createStringAvroConsumer(consumerProps)
      val records = consumeStringAvro(consumer, topic, objects.length)
      objects.toSeq shouldBe records
    }

    "handle the avro new producer" in {
      val topic = "testAvro"
      val avroRecord = createAvroRecord
      val objects = Array[Any](avroRecord, true, 130, 345L, 1.23f, 2.34d, "abc", "def".getBytes)
      val producerProps = stringAvroProducerProps
      val producer = createProducer[String,Any](producerProps)
      for (o <- objects) {
        producer.send(new ProducerRecord[String, Any](topic, o))
      }
      val consumerProps = stringAvroConsumerProps()
      val consumer = createStringAvroConsumer(consumerProps)
      val records = consumeStringAvro(consumer, topic, objects.length)
      objects.deep shouldBe records.toArray.deep
    }
  }

}
