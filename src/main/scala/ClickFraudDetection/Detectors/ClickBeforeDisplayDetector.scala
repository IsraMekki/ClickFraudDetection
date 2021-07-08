package ClickFraudDetection.Detectors


import ClickFraudDetection.Event
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

case class ClickBeforeDisplayDetector( )

object ClickBeforeDisplayDetector {

  def process(
             clicks: DataStream[Event],
             displays: DataStream[Event],
             windowSize: Long,
             tolerated_time_diff: Long): DataStream[Event] = {

    //with window size equal to 10 seconds and tolerance to 5: mostly 238.186.83.58 appears as fraudulents, but there's others to,
    //sometimes it takes time for these fraudulent events to appear, they seem to be rare compared to other found with previous filters
    //same with window size of 5 seconds

    //Pattern 2 :
    //si par impression id, par uid,
    //un click apparait tout seul dans son coin sans display autour (10 secondes avant ou après) fraude
    val clicks_by_uid_imp : KeyedStream[Event, (String,String)]= clicks
      .keyBy(d => (d.uid,d.impressionId))

    val displays_by_uid_imp : KeyedStream[Event, (String,String)] = displays
      .keyBy(d => (d.uid, d.impressionId))

    //they look like fraud because clicks appear near displays
    // NB. It always seems to be the same ip address (the one we filtered in suspiciousIpDetector)
    val fraud_joined: DataStream[(Event,Event)] = clicks_by_uid_imp.join(displays_by_uid_imp)
      .where(c => (c.uid,c.impressionId)).equalTo(d => (d.uid,d.impressionId))
      .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
      .apply { (e1, e2) => (e1, e2) }
    //fraud_joined.print()

    val filtered_fraud_joined = fraud_joined filter (t => t._1.timestamp < (t._2.timestamp - tolerated_time_diff))
    //val fraud_clicks : DataStream[Event] = filtered_fraud_joined.map{ d => (d._1)}
    filtered_fraud_joined.map{ d => (d._1)}.writeAsText("frauds/ClickBeforeDisplayEvents", writeMode = WriteMode.OVERWRITE).setParallelism(1)

    val filtered_clean_joined = fraud_joined filter (t => t._1.timestamp >= (t._2.timestamp - tolerated_time_diff))
    filtered_clean_joined.map(d => d._1)


  }
}

