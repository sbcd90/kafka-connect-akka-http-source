package io.confluent.connect

import java.util

import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse}
import io.confluent.connect.avro.AvroConverter
import io.confluent.connect.util.Version
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class AkkaHttpSourceTask extends SourceTask {
  private var producer: KafkaProducer[Array[Byte], Array[Byte]] = _

  // name of the constants
  private val topicname = "kafka.topic"
  private val servers = "kafka.bootstrap.servers"

  // should be taken as input from user
  private var topic: String = _
  private var bootstrap_servers: String = _
  private val schema = Schema.STRING_SCHEMA

  // custom offset handled by source
  private var offset: Int = 0

  override def start(map: java.util.Map[String, String]): Unit = {
    topic = map.get(topicname)
    bootstrap_servers = map.get(servers)

    // copying settings from kafka source.
    val producerProps = new java.util.HashMap[String, Object]()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers)
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")

    // These settings are designed to ensure there is no data loss.
    producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE.toString)
    producerProps.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE.toString)
    producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MaxValue.toString)
    producerProps.put(ProducerConfig.ACKS_CONFIG, "all")
    producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")

    producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)

    offset += 1
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
    val key = new AvroConverter().fromConnectData(record.topic(), record.keySchema(), record.key())
    val value = new AvroConverter().fromConnectData(record.topic(), record.valueSchema(), record.value())

    val producerRecord = new ProducerRecord[Array[Byte], Array[Byte]](record.topic(), record.kafkaPartition(), key, value)
    producer.send(producerRecord)
  }

  def asyncHandler(request: HttpRequest): Future[HttpResponse] = {
    request match {
      case HttpRequest(HttpMethods.GET, _, _, _, _) =>
        Future[HttpResponse] {
          val sourcePartitions = new java.util.HashMap[String, String]()
          sourcePartitions.put("request", "akka-http")

          val sourceOffsets = new java.util.HashMap[String, String]()
          sourceOffsets.put("pos", offset.toString)

          val record = new SourceRecord(sourcePartitions, sourceOffsets, topic, schema, "Hello World" + offset)

          commitRecord(record)
          offset += 1

          HttpResponse(entity = "Hello World")
        }
    }
  }
}