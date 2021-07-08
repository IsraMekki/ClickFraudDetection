# Contents
This project is structured as follows:
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
|_test_results
              |image.jpg
              |_...
```

## Offline analysis
**offline_analysis.ipynb** is a python notebook where we analysed a finite stream. We ran a simple flink program for approx. 3h, where we collected clicks and displays and outputted them into files (in test_results/clicks and test_results/displays).
This analysis allowed us to extract probable patterns, which we ended up implementing (See the details [here](../master/src/main/scala/ClickFraudDetection/README.md)).
## Scala files
Our implementation can be found in [ClickFraudDetection](../master/src/main/scala/ClickFraudDetection/).
* **Event** is a class which converts Strings to objects with meaningful properties (eventType, uid, timestamp, ip and impressionId) with the correct types. We work with Streams of Events to simplify our implementation.
* **WaterMarkAssigner** alows to create watermarks for event time processing based on timestamps in collected streams.
* **ClickFraudDetectionJob** main class where the program is launched.
* **CTRCalculator** allows to calculate the Click Through Rate from clicks and displays streams.
* **Detectors** contains classes where our filters are implemented. Each class takes as input a (or 2) DataStream(s) of Event and gives as output a DataStream of **cleaned** Events. The fraudulant ones are written into a text file.

# To run this project
Run the main job in 
```
src/main/scala/ClickFraudDetection/ClickFraudDetectionJob.scala
```
# Results 
Our principal metric was the CTR by UID. We ran de click fraud detection job for some time (~1h) and printed the CTRs in files (CTR_UID_NO_PROCESSING contains the CTR of the initial clicks and displays streams, and CTR_UID_POST_PROCESSING is the CTR after applying our filters.) We then calculate the average CTR for each file:

| CTR_UID_NO_PROCESSING | CTR_UID_POST_PROCESSING |
|-----------------------|-------------------------|
| 79.33%                | 15.37%                  |

# What we learnt from this project
* Introduction to the Pay-Per Click model (PPC)
* Awareness of the presence of fraud, and the importance of analyzing data to extract fraudulant patterns.
* Introduction to streaming programming in practice with Kafka and Flink.
* First experience using Scala
* Streaming is not easy 😅
