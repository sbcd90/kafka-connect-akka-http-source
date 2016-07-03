package io.confluent.connect.util

import akka.http.scaladsl.model.HttpResponse
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

class KafkaCallback {
  var httpResponse: HttpResponse = _

  def getCallback() : Callback = {
    new Callback() {
      override def onCompletion(recordMetadata: RecordMetadata, ex: Exception): Unit = {
        if (ex != null) {
          httpResponse = HttpResponse(entity = ex.getMessage, status = 400)
        } else {
          println(recordMetadata.topic() + " - " + recordMetadata.partition() + " - " + recordMetadata.offset())
          httpResponse = HttpResponse(entity = recordMetadata.topic() + " - " + recordMetadata.partition()
            + " - " + recordMetadata.offset(), status = 200)
        }
      }
    }
  }
}