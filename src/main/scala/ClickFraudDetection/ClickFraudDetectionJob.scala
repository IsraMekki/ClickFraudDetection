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

import ClickFraudDetection.Detectors.TooManyIps2Detector
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import ClickFraudDetection.Detectors.SuspiciousIpDetector

import java.util.Properties

object ClickFraudDetectionJob {
    def main(args: Array[String]) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        val properties = new Properties()
        properties.setProperty("bootstrap.servers", "localhost:9092")
        properties.setProperty("group.id", "test")

        val clickStream = env
                .addSource(new FlinkKafkaConsumer[String]("clicks", new SimpleStringSchema(), properties))

        //val fraudIpStream = SuspiciousIpDetector().process(
        //    clickStream.map(line => Event.create(line)))

        //fraudIpStream.print()

        val fraudTooManyIp = clickStream.map(line => Event.create(line))
                .keyBy(event => event.uid)
                .process(new TooManyIps2Detector).setParallelism(1)

        //val fraudTooManyIp = TooManyIpsDetector().process(clickStream.map(line => Event.create(line)))
        //fraudTooManyIp.writeAsText("too_many_ips", WriteMode.OVERWRITE).setParallelism(1)

        fraudTooManyIp.print()

        env.execute("Flink Streaming Scala API Skeleton")
    }
}
