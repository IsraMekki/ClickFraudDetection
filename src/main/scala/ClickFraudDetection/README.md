# Overview of the main Job
As depicted in the following, the main job uses the implemented detectors one after another. Each detector takes as input a stream containing fraudulant Events and outputs fraud free (w.r.t the pattern it filters) Event streams. The latter become inputs to the next detector, etc.
We chose the following order based on each pattern's frequency.
![Main job overview](https://github.com/IsraMekki/ClickFraudDetection/blob/master/test_results/CFD.png?raw=true)

# Implemented Detectors
## SuspiciousIpDetector
## TooManyClicksDetector
## ClickBeforeDisplayDetector
## TooManyIpsDetector
## TooManyIps2Detector

# CTRCalculator
