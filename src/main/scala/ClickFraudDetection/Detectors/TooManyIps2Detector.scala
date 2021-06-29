package ClickFraudDetection.Detectors

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import TooManyIps2Detector.maxIpPerUser
import ClickFraudDetection.Event

import scala.collection.mutable.ListBuffer


// When a uid is associated with too many IP addresses
// Implementation with KeyedProcessFunction

object TooManyIps2Detector {
    val maxIpPerUser: Int = 2
    val ONE_MINUTE: Long     = 60 * 1000L
}

@SerialVersionUID(1L)
class TooManyIps2Detector extends KeyedProcessFunction[String, Event, Event] {

    private var prevIpStates: ListBuffer[ValueState[String]] = new ListBuffer[ValueState[String]]()

    @throws[Exception]
    override def open(parameters: Configuration): Unit = {
        var flagDescriptors = new ListBuffer[ValueStateDescriptor[String]]()
        for( i <- 0 until maxIpPerUser){
            val flagDescriptor = new ValueStateDescriptor("flag" + i.toString, Types.STRING)
            flagDescriptors += flagDescriptor
            prevIpStates += getRuntimeContext.getState(flagDescriptor)
        }

    }

    override def processElement(
                                       event: Event,
                                       context: KeyedProcessFunction[String, Event, Event]#Context,
                                       collector: Collector[Event]): Unit = {

        // Get the current state for the current key
        val prevIPs = new ListBuffer[String]()
        var nextNullPos = maxIpPerUser
        for( i <- 0 until maxIpPerUser){
            prevIPs += prevIpStates(i).value
            if (prevIPs(i) == null) nextNullPos = i
        }

        // Check if the flag is set
        if (nextNullPos == maxIpPerUser) {
            if (!prevIPs.contains(event.ip)){
                // Output an alert downstream
                val problematicEvent = event

                collector.collect(problematicEvent)
            }
            // Clean up our state
            cleanUp(context)
            nextNullPos = 0
        }

        if (!prevIPs.contains(event.ip)) prevIpStates(nextNullPos).update(event.ip)


    }

    @throws[Exception]
    private def cleanUp(ctx: KeyedProcessFunction[String, Event, Event]#Context): Unit = {
        // delete timer
        for( i <- 0 until maxIpPerUser){
            prevIpStates(i).clear()
        }
    }
}
