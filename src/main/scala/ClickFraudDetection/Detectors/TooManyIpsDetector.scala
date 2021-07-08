package ClickFraudDetection.Detectors

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import TooManyIpsDetector.{maxIpsPerUser, windowSize}
import ClickFraudDetection.{Event, WaterMarkAssigner}
import ClickFraudDetection.Event
import akka.stream.impl.fusing.Sliding
import org.apache.flink.core.fs.FileSystem.WriteMode

object TooManyIpsDetector {
    val maxIpsPerUser: Int = 3
    val windowSize: Int = 30    //seconds
}

// When a uid is associated with too many IP addresses
// => Probably bots ?
case class TooManyIpsDetector() {
    def process(eventStream: DataStream[Event]): DataStream[Event] = {
        val processedEvents = eventStream.map(event => ((event.uid, event.ip), event, 1))
                .keyBy(_._1)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSize * 2)))
                .reduce{ (x, y) => (x._1, x._2, 1) }
                .keyBy(_._1._1)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                .reduce{ (x, y) => (x._1, x._2, x._3 + y._3) }

        processedEvents.filter(_._3 > maxIpsPerUser).map(_._2).writeAsText("frauds/TooManyIpsEvents-" + eventStream.name, writeMode = WriteMode.OVERWRITE).setParallelism(1)

        processedEvents.filter(_._3 <= maxIpsPerUser).map(_._2)

    }
}
