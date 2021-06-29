package ClickFraudDetection

import scala.util.parsing.json.JSON

case class Event(eventType: String,
                 uid: String,
                 timestamp: Long,
                 ip: String,
                 impressionId: String)
object Event{
    def create(line: String): Event = {
        val result = JSON.parseFull(line)

        val eventType = result match { case Some(m: Map[String, String]) => m("eventType") }
        val uid = result match { case Some(m: Map[String, String]) => m("uid") }
        val timestamp = result match { case Some(m: Map[String, Double]) => m("timestamp") }
        val ip = result match { case Some(m: Map[String, String]) => m("ip") }
        val impressionId = result match { case Some(m: Map[String, String]) => m("impressionId") }

        Event(eventType, uid, timestamp.toLong, ip, impressionId)
    }
}