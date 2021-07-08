
package ClickFraudDetection.Detectors

import org.apache.flink.streaming.api.scala.DataStream
import SuspiciousIpDetector.suspiciousIp
import ClickFraudDetection.Event
import org.apache.flink.core.fs.FileSystem.WriteMode


object SuspiciousIpDetector {
    val suspiciousIp: String = "238.186.83.58"
}

// We noticed that 238.186.83.58 comes back a lot
// checked on the internet and it seems unassigned
// => probably a fraud

case class SuspiciousIpDetector() {
    def process(eventStream: DataStream[Event]): DataStream[Event] = {
        val suspiciousIpStream = eventStream.filter(_.ip == suspiciousIp)
        suspiciousIpStream.writeAsText("frauds/SuspiciousIpEvents-" + eventStream.name, writeMode = WriteMode.OVERWRITE).setParallelism(1)
        eventStream.filter(_.ip != suspiciousIp)
    }
}
