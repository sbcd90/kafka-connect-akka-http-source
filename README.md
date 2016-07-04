kafka-connect-akka-http-source
==============================

- An akka-http source connector plugin for Kafka Connect.

### Installation

```
mvn clean install
```
Get the jar & put into the location pointed by `CLASSPATH` environment variable.
See [installing custom plugins](http://docs.confluent.io/3.0.0/connect/userguide.html#installing-connector-plugins)

### Run the plugin in standalone mode
 
```
./bin/connect-standalone connect-avro-standalone.properties connect-akka-http-source.properties 
```

```
POST http://localhost:8085/post
payload: "Hello World"
```

### External configurations

```
src/main/resources/kafka-connect-akka-http-source.properties
connect-akka-http-source.properties
```
