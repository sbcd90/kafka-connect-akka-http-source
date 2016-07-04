package io.confluent.connect

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import io.confluent.connect.util.Version
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.source.SourceConnector

class AkkaHttpSourceConnector extends SourceConnector {
  private val HOST_CONFIG = "akka.http.hostname"
  private val PORT_CONFIG = "akka.http.port"

  private var hostname: String = _
  private var port: String = _

  implicit val actorSystem = ActorSystem("Akka-Http-Kafka-Connect-source-connector")
  implicit val fm = ActorMaterializer()

  override def version(): String = {
    Version.getVersion()
  }

  override def taskClass() = classOf[AkkaHttpSourceTask]

  override def start(map: java.util.Map[String, String]): Unit = {
    hostname = map.get(HOST_CONFIG)
    port = map.get(PORT_CONFIG)
    Http().bindAndHandleAsync(new AkkaHttpSourceTask().asyncHandler, hostname, port.toInt)
  }

  override def stop(): Unit = {

  }

  override def taskConfigs(i: Int): java.util.List[java.util.Map[String, String]] = {
    val configs = new java.util.ArrayList[java.util.Map[String, String]]()

    val config = new java.util.HashMap[String, String]()
    if (hostname != null) {
      config.put(HOST_CONFIG, hostname)
    }
    if (port != null) {
      config.put(PORT_CONFIG, port)
    }
    configs.add(config)
    configs
  }

  override def config: ConfigDef = {
    new ConfigDef()
  }
}