package ClickFraudDetection.Detectors

package org.myorg.quickstart
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

case class TooManyClicksDetector( )

object TooManyClicksDetector {

  def build(
             clicks: DataStream[JsonNode],
             windowSize: Long): Unit = {

    //with a restrictive time window of 2 seconds and a minimum nb of clicks per uid equal to more or less 3
    //we get some fraudulent clicks

    //Pattern 1 :
    //on the [[Clicks]] queue, if more than 5 clicks occur from the same [[uid]] dans une tumbling window id.
    val clicks_by_uid: DataStream[(JsonNode, Int)] = clicks
      .map {
        (_, 1)
      }
      .keyBy(d => d._1.get("uid").asText())
      .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
      .reduce { (v1, v2) => (v1._1, v1._2 + v2._2) }
    val uid_fraud_clicks = (clicks_by_uid filter (c => (c._2 >= 3) )) // in 5 seconds more than 2 clicks, i have seen 4

    uid_fraud_clicks.print()

  }
}
