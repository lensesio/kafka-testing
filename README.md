[![Build Status](https://travis-ci.org/Landoop/kafka-testing.svg?branch=master)](https://travis-ci.org/Landoop/kafka-testing) 
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.landoop/kafka-testing/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.landoop/kafka-testing_2.11)

# Kafka Unit Testing

Allows you to start and stop for unit testing applications that communicate with Kafka `one or more Kafka brokers + a ZooKeeper instance + a Schema Registry instance + a Kafka Connect instance`

## Versions

| kafka-testing | Kafka broker              | Zookeeper | Schema Registry | Kafka Connect |
|---------------|---------------------------|-----------| ----------------| --------------|
| 0.1           | kafka_2.11 : 0.10.2.0     | 3.4.6     |           3.2.0 |         3.2.0 |
| 0.2           | kafka_2.11 : 0.10.2.1-cp2 | 3.4.6     |           3.2.2 |         3.2.2 |

## Maven central

```xml
<dependency>
  <groupId>com.landoop</groupId>
  <artifactId>kafka-testing_2.11</artifactId>
  <version>0.2</version>
</dependency>
```

```gradle
compile 'com.landoop:kafka-testing_2.11:0.2
```

```sbt
libraryDependencies += "com.landoop" %% "kafka-testing" % "0.2"
```

## Using it

```scala
 val kafkaCluster: KCluster = new KCluster()
 
 //get kafka brokers
 val brokers = kafkaCluster.BrokersList
 
 //get schema registry client
 val schemaRegistryClient = kafkaCluster.SchemaRegistryService.get.RestClient
 
 
 //get schema registry endpoint
 val schemaRegistryClient = kafkaCluster.SchemaRegistryService.get.Endpoint
 
 //get Zookeeper Client
 val zkClient = kafkaCluster.ZKClient
 
 //start connect
 kafkaCluster.startEmbeddedConnect(...)
```

## License

```
Copyright 2017 Landoop

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
