package io.confluent.connect

import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse}
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class AkkaHttpSourceTask extends SourceTask {
  private val topic = "test8"
  private val schema = Schema.STRING_SCHEMA
  private var requestCount: Int = 0

  override def start(map: java.util.Map[String, String]): Unit = {
    requestCount += 1
  }

  override def stop(): Unit = {

  }

  override def poll(): java.util.List[SourceRecord] = {
    val sourcePartitions = new java.util.HashMap[String, String]()
    sourcePartitions.put("request", "akka-http")

    val sourceOffsets = new java.util.HashMap[String, String]()
    sourceOffsets.put("pos", requestCount.toString)

    val record = new SourceRecord(sourcePartitions, sourceOffsets, topic, schema, "Hello World")

    val records = new java.util.ArrayList[SourceRecord]()
    records.add(record)
    requestCount += 1

    records
  }

  override def version(): String = {
    null
  }

  def asyncHandler(request: HttpRequest): Future[HttpResponse] = {
    request match {
      case HttpRequest(HttpMethods.GET, _, _, _, _) =>
        Future[HttpResponse] {
          HttpResponse(entity = "Hello World")
        }
    }
  }
}