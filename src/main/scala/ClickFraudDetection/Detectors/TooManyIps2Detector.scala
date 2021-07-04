package ClickFraudDetection.Detectors

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import TooManyIps2Detector.maxIpPerUser
import ClickFraudDetection.Event

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._


// When a uid is associated with too many IP addresses
// Implementation with KeyedProcessFunction

object TooManyIps2Detector {
    val maxIpPerUser: Int = 10
    val windowSize: Long     = 60 * 1000L
}

@SerialVersionUID(1L)
class TooManyIps2Detector extends KeyedProcessFunction[String, Event, Event] {

    private var prevIpStates: ListBuffer[ValueState[String]] = new ListBuffer[ValueState[String]]()
    @transient private var timerState: ValueState[java.lang.Long] = _

    @throws[Exception]
    override def open(parameters: Configuration): Unit = {
        var flagDescriptors = new ListBuffer[ValueStateDescriptor[String]]()
        for( i <- 0 until maxIpPerUser){
            val flagDescriptor = new ValueStateDescriptor("flag" + i.toString, Types.STRING)
            flagDescriptors += flagDescriptor
            prevIpStates += getRuntimeContext.getState(flagDescriptor)
        }

        val timerDescriptor = new ValueStateDescriptor("timer-state", Types.LONG)
        timerState = getRuntimeContext.getState(timerDescriptor)

    }

    override def processElement(
                                       event: Event,
                                       context: KeyedProcessFunction[String, Event, Event]#Context,
                                       collector: Collector[Event]): Unit = {

        // Get the current state for the current key
        val prevIPs = new ListBuffer[String]()
        var nextNullPos = maxIpPerUser
        breakable{
            for( i <- 0 until maxIpPerUser){
                prevIPs += prevIpStates(i).value
                if (prevIPs(i) == null){
                    nextNullPos = i
                    break
                }
            }
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
        if (nextNullPos == 0){
            val timer = context.timerService.currentWatermark() + TooManyIps2Detector.windowSize

            context.timerService.registerEventTimeTimer(timer)
            timerState.update(timer)
        }


    }

    override def onTimer(
                                timestamp: Long,
                                ctx: KeyedProcessFunction[String, Event, Event]#OnTimerContext,
                                out: Collector[Event]): Unit = {
        // remove flag after 1 minute
        timerState.clear()
        for( i <- 0 until maxIpPerUser){
            prevIpStates(i).clear()
        }
    }

    @throws[Exception]
    private def cleanUp(ctx: KeyedProcessFunction[String, Event, Event]#Context): Unit = {// delete timer
        val timer = timerState.value
        ctx.timerService.deleteEventTimeTimer(timer)

        // clean up all states
        timerState.clear()

        for( i <- 0 until maxIpPerUser){
            prevIpStates(i).clear()
        }
    }
}
