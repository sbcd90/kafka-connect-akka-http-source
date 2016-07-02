package io.confluent.connect.util

import com.typesafe.config.ConfigFactory

object Version {
  private val version = ConfigFactory.load("kafka-connect-akka-http-version.properties")
    .getString("version")

  def getVersion(): String = {
    version
  }
}