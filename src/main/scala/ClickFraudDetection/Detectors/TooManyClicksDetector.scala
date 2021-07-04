package ClickFraudDetection.Detectors

import ClickFraudDetection.Event
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

case class TooManyClicksDetector()

object TooManyClicksDetector {

    def process(clicks: DataStream[Event],
                windowSize: Long = 5,
                maxClicks: Int = 3): DataStream[Event] = {

        //with a restrictive time window of 2 seconds and a minimum nb of clicks per uid equal to more or less 3
        //we get some fraudulent clicks

        //Pattern 1 :
        //on the [[Clicks]] queue, if more than 5 clicks occur from the same [[uid]] dans une tumbling window id.
        val clicks_by_uid: DataStream[(Event, Int)] = clicks
                .map {
                    (_, 1)
                }
                .keyBy(d => d._1.uid)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                .reduce { (v1, v2) => (v1._1, v1._2 + v2._2) }
        val uid_fraud_clicks = (clicks_by_uid filter (c => (c._2 >= maxClicks))) // in 5 seconds more than 2 clicks, i have seen 4

        uid_fraud_clicks.map(e => e._1).writeAsText("TooManyClicksEvents")
        val uid_clean_clicks = clicks_by_uid filter (c => (c._2 < maxClicks))
        uid_clean_clicks.map(x => x._1)

    }
}
