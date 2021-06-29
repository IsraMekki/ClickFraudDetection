package ClickFraudDetection

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class WaterMarkAssigner extends AssignerWithPunctuatedWatermarks[Event] {
    override def extractTimestamp(element: Event, previousElementTimestamp: Long): Long = {
        element.timestamp * 1000
    }
    override def checkAndGetNextWatermark(lastElement: Event, extractedTimestamp: Long): Watermark = {
        new Watermark(extractedTimestamp)
    }
}