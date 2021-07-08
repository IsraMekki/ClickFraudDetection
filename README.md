# Contents
The main parts of this project are structured as follows:
```
|_README.md
|_offline_analysis.ipynb
|_src
     |_...
           |_ClickFraudDetection
                                |_Detectors
                                           |_ClickBeforeDisplayDetector.scala
                                           |_SuspiciousIpDetector.scala
                                           |_TooManyClicksDetector.scala
                                           |_TooManyIps2Detector.scala
                                           |_TooManyIpsDetector.scala
                                |_ClickFraudDetectionJob.scala
                                |_Event.scala
                                |_WaterMarkAssigner.scala
                                |_CTRCalculator.scala
                                |_README.md
|_test_results
              |cliks
              |_displays
              |_CTR_UID_NO_PROCESSING
              |_CTR_UID_POST_PROCESSING
```

## Offline analysis
**offline_analysis.ipynb** is a python notebook that has been used to analyse a finite stream. We have run a simple flink program for approx. 3h, during which we have collected Kafka clicks and displays, and outputted them into files (test_results/clicks and test_results/displays).
This analysis has enabled us to extract/notice probable fraudulent patterns, which we have ended up identifying through an online analysis implemented [here](../master/src/main/scala/ClickFraudDetection/README.md)).
## Scala files
Our implementation can be found in [ClickFraudDetection](../master/src/main/scala/ClickFraudDetection/).
* **Event** is a class which converts Strings to objects with meaningful properties (eventType, uid, timestamp, ip and impressionId) with the correct types. We work with Streams of Events to simplify our implementation.
* **WaterMarkAssigner** creates watermarks for event time processing based on timestamps present in the collected stream events.
* **ClickFraudDetectionJob** is the main class from where the program can be launched.
* **CTRCalculator** computes the Click Through Rate from clicks and displays streams.
* **Detectors** contains classes where our filters are implemented. Each class takes as input a (or 2) DataStream(s) of Event and outputs a DataStream of **cleaned** Events. The fraudulant ones are written into a text file.

# To run this project
Run the main job in 
```
src/main/scala/ClickFraudDetection/ClickFraudDetectionJob.scala
```
# Results 
Our principal metric has been the CTR by UID. We have run the click fraud detection job for some time (~1h) and have outputted the CTRs into files (CTR_UID_NO_PROCESSING contains the CTR of the initial clicks and displays streams, and CTR_UID_POST_PROCESSING contains the CTR after applying our filters). We have then computed the average CTR for each file:

| CTR_UID_NO_PROCESSING | CTR_UID_POST_PROCESSING |
|-----------------------|-------------------------|
| 79.33%                | 15.37%                  |

# What we have learned from this project
* Introduction to the Pay-Per Click model (PPC).
* Awareness of the presence of fraud, and the importance of analyzing real-time data to extract/notice fraudulent patterns.
* Introduction to streaming programming in practice with Kafka and Flink.
* First experience using Scala.
* Streaming is not easy 😅
