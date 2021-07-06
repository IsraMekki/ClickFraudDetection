package ClickFraudDetection

import ClickFraudDetection.CTRCalculator.windowSize
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object CTRCalculator {
    val windowSize: Int = 2   //minutes
}

// When a uid is associated with too many IP addresses
// => Probably bots ?
case class CTRCalculator() {
    def getCTR(clickStream: DataStream[Event], displayStream: DataStream[Event]): DataStream[Double] = {
        val keyedClickStream = clickStream
                .map(x => (x, 1))
                //.keyBy(c => (c._1.uid, c._1.impressionId))
                .keyBy(c => c._1.uid)
                //.window(TumblingEventTimeWindows.of(Time.minutes(windowSize)))
                .reduce { (c1, c2) => (c1._1, c1._2 + c2._2) }

        val keyedDisplayStream = displayStream
                .map(x => (x, 1))
                .keyBy(d => d._1.uid)
                //.window(TumblingEventTimeWindows.of(Time.minutes(windowSize)))
                .reduce { (d1, d2) => (d1._1, d1._2 + d2._2) }

        // Join both keyed reduced streams to compute CTR by UID and impressionID during the defined time window
        val joined = keyedClickStream.join(keyedDisplayStream)
                .where(c => c._1.uid)
                .equalTo(d => d._1.uid)
                .window(TumblingEventTimeWindows.of(Time.minutes(windowSize)))
                //.apply { (e1, e2) => e1._2.toDouble }
                .apply { (e1, e2) => (e1._2/e2._2.toDouble)} //CTR = nb clicks/nb displays

        joined
    }
}
