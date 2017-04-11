
# Kafka-Testing
A library allowing you to run a Kafka cluster within your JVM! 
The cluster is composed of:
* 1(default) or more brokers (!mandatory)
* 1 Zookeeper instance (!mandatory)
* Schema Registry and its default compatibility policy
* Kafka Connect workers.


### Import it into your project
```json
//maven
<dependency>
  <groupId>com.landoop</groupId>
  <artifactId>kafka-testing_2.11</artifactId>
  <version>0.1</version>
</dependency>


//gradle
compile 'com.landoop:kafka-testing_2.11:0.1

//sbt
libraryDependencies += "com.landoop" %% "kafka-testing" % "0.1"
```
###How to use it:

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

Simple! 
Happy testing...

## Release History

0.1 - first cut (2017-04-11)

