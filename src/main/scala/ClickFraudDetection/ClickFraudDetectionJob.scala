/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ClickFraudDetection

import org.apache.flink.api.common.serialization.SimpleStringSchema
import ClickFraudDetection.Detectors.{ClickBeforeDisplayDetector, SuspiciousIpDetector, TooManyClicksDetector, TooManyIps2Detector, TooManyIpsDetector}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties
import scala.util.parsing.json.JSON

object ClickFraudDetectionJob {
    def main(args: Array[String]) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "localhost:9092")
        properties.setProperty("group.id", "test")

        val clickStream = env
                .addSource(new FlinkKafkaConsumer[String]("clicks", new SimpleStringSchema(), properties))
                .map(line => Event.create(line))
                .assignTimestampsAndWatermarks(new WaterMarkAssigner)
                .name("clicks")

        val displayStream = env
                .addSource(new FlinkKafkaConsumer[String]("displays", new SimpleStringSchema(), properties))
                .map(line => Event.create(line))
                .assignTimestampsAndWatermarks(new WaterMarkAssigner)
                .name("displays")

        val cleanedClicks1 = SuspiciousIpDetector().process(clickStream).name("clicks1")
        //val cleanedDisplays1 = SuspiciousIpDetector().process(displayStream).name("displays1")

        val cleanedClicks2 = TooManyClicksDetector.process(cleanedClicks1).name("clicks2")

        val cleanedClicks3 = TooManyIpsDetector().process(cleanedClicks2).name("clicks3")
        //val cleanedDisplays3 = TooManyIpsDetector().process(displayStream).name("displays3")

        //val cleanedClicks4 = ClickBeforeDisplayDetector.process(cleanedClicks3, cleanedDisplays3, 5, 2)

        val ctr = CTRCalculator().getCTR(cleanedClicks3, displayStream)
        ctr.writeAsText("CTR_UID_POST_PROCESSING").setParallelism(1)

        val ctr_before = CTRCalculator().getCTR(clickStream, displayStream)
                .writeAsText("CTR_UID_NO_PROCESSING").setParallelism(1)
        env.execute("Click Fraud Detection Job")
    }
}
