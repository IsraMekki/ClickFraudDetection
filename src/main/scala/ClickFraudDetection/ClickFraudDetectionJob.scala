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

import ClickFraudDetection.Detectors.{ClickBeforeDisplayDetector, SuspiciousIpDetector, TooManyClicksDetector, TooManyIps2Detector, TooManyIpsDetector}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode

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

        val displayStream = env
                .addSource(new FlinkKafkaConsumer[String]("displays", new SimpleStringSchema(), properties))
                .map(line => Event.create(line))
                .assignTimestampsAndWatermarks(new WaterMarkAssigner)

        val cleaned1 = SuspiciousIpDetector().process(clickStream)
        val cleaned2 = TooManyClicksDetector.process(cleaned1)
        val cleaned3 = TooManyIpsDetector().process(cleaned2)
        //val cleaned3 = clickStream.keyBy(event => event.uid).process(new TooManyIps2Detector) TODO: adapt (or not)
        val cleaned4 = ClickBeforeDisplayDetector.process(cleaned3, displayStream, 5, 2)   //parameters to correct

        cleaned4.writeAsText("CleanEvents").setParallelism(1)

        //TODO: Calculate CTR https://www.ververica.com/blog/real-time-performance-monitoring-with-flink-sql-ad-tech-use-case


        env.execute("Click Fraud Detection Job")
    }
}
