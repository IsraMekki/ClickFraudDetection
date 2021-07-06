package ClickFraudDetection.Detectors

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import TooManyIpsDetector.{maxIpsPerUser, windowSize}
import ClickFraudDetection.{Event, WaterMarkAssigner}
import ClickFraudDetection.Event

object TooManyIpsDetector {
    val maxIpsPerUser: Int = 3
    val windowSize: Int = 1    //minutes
}

// When a uid is associated with too many IP addresses
// => Probably bots ?
case class TooManyIpsDetector() {
    def process(eventStream: DataStream[Event]): DataStream[Event] = {
        val processedEvents = eventStream.map(event => ((event.uid, event.ip), event, 1))
                .keyBy(_._1)
                .window(TumblingEventTimeWindows.of(Time.minutes(windowSize)))
                .reduce{ (x, y) => (x._1, x._2, 1) }
                .keyBy(_._1._1)
                .window(TumblingEventTimeWindows.of(Time.minutes(windowSize)))
                .reduce{ (x, y) => (x._1, x._2, x._3 + y._3) }

        processedEvents.filter(_._3 > maxIpsPerUser).map(_._2).writeAsText("TooManyIpsEvents").setParallelism(1)

        processedEvents.filter(_._3 <= maxIpsPerUser).map(_._2)

    }
}
