package ClickFraudDetection

import ClickFraudDetection.CTRCalculator.windowSize
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object CTRCalculator {
    val windowSize: Int = 1    //minutes
}

// When a uid is associated with too many IP addresses
// => Probably bots ?
case class CTRCalculator() {
    def getCTR(clickStream: DataStream[Event], displayStream: DataStream[Event]): DataStream[Double] = {
        val keyedClickStream = clickStream
                .map(x => (x, 1))
                .keyBy(d => (d._1.uid, d._1.impressionId))

        val keyedDisplayStream = displayStream
                .map(x => (x, 1))
                .keyBy(d => (d._1.uid, d._1.impressionId))

        keyedClickStream.join(keyedDisplayStream)
                .where(c => (c._1.uid,c._1.impressionId))
                .equalTo(d => (d._1.uid,d._1.impressionId))
                .window(TumblingEventTimeWindows.of(Time.minutes(windowSize)))
                .apply { (e1, e2) => e1._2.toDouble }


    }
}
