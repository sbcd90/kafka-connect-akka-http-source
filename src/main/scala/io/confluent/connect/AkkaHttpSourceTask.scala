package io.confluent.connect

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse, Uri}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.StreamConverters
import com.typesafe.config.ConfigFactory
import io.confluent.connect.avro.AvroConverter
import io.confluent.connect.util.{KafkaCallback, Version}
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.commons.io.IOUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class AkkaHttpSourceTask extends SourceTask {
  implicit val actorSystem = ActorSystem("Akka-Http-Kafka-Connect-source-task")
  implicit val fm = ActorMaterializer()

  private val producerProps: java.util.Map[String, Object] =
    new java.util.HashMap[String, Object]()
  private val kafkaCallback = new KafkaCallback()

  // name of the constants
  private val topicname = "kafka.topic"
  private val servers = "kafka.bootstrap.servers"
  private val schemaregurlname = "kafka.schema.registry.url"

  // read config file
  private val map = ConfigFactory.load("kafka-connect-akka-http-source.properties")

  // should be taken as input from user
  private val topic = map.getString(topicname)
  private val bootstrap_servers = map.getString(servers)
  private val schemaregurl = map.getString(schemaregurlname)

  // will be filled by class implementor
  var schema = Schema.STRING_SCHEMA

  // custom offset handled by source
  private var offset: Int = 1

  override def start(map: java.util.Map[String, String]): Unit = {
  }

  override def stop(): Unit = {

  }

  override def poll(): java.util.List[SourceRecord] = {
    null
  }

  override def version(): String = {
    Version.getVersion()
  }

  override def commitRecord(record: SourceRecord): Unit = {
    val config = new java.util.HashMap[String, Object]()
    config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaregurl)

    val converter = new AvroConverter()
    converter.configure(config, false)

    // copying settings from kafka source.
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers)
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")

    // These settings are designed to ensure there is no data loss.
    producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE.toString)
    producerProps.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE.toString)
    producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MaxValue.toString)
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all")
    producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")

    println(producerProps.entrySet().size())
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)
    val key = converter.fromConnectData(record.topic(), record.keySchema(), record.key())
    val value = converter.fromConnectData(record.topic(), record.valueSchema(), record.value())

    val producerRecord = new ProducerRecord[Array[Byte], Array[Byte]](record.topic(), record.kafkaPartition(), key, value)
    producer.send(producerRecord, kafkaCallback.getCallback())
  }

  def asyncHandler(request: HttpRequest): Future[HttpResponse] = {
    request match {
      case HttpRequest(HttpMethods.POST, Uri.Path("/post"), _, _, _) =>
        Future[HttpResponse] {
          val body = IOUtils.toString(request.entity.dataBytes.runWith(
            StreamConverters.asInputStream(FiniteDuration(3, TimeUnit.SECONDS))))
          println(body)

          val sourcePartitions = new java.util.HashMap[String, String]()
          sourcePartitions.put("request", "akka-http")

          val sourceOffsets = new java.util.HashMap[String, String]()
          sourceOffsets.put("pos", offset.toString)

          val record = new SourceRecord(sourcePartitions, sourceOffsets, topic, schema, body)

          commitRecord(record)
          offset += 1

          var httpResponse = kafkaCallback.httpResponse
          while (httpResponse == null) {
            httpResponse = kafkaCallback.httpResponse
          }
          httpResponse
        }
      case HttpRequest(_, _, _, _, _) =>
        Future[HttpResponse] {
          HttpResponse(entity = "Not implemented", status = 200)
        }
    }
  }
}