package io.confluent.connect

import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse}
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class AkkaHttpSourceTask extends SourceTask {

  override def start(map: java.util.Map[String, String]): Unit = {

  }

  override def stop(): Unit = {

  }

  override def poll(): java.util.List[SourceRecord] = {
    null
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