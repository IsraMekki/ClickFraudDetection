## Overview of the main Job
As depicted in the following, the main job uses the implemented detectors one after another. Each detector takes as input a stream containing fraudulant Events and outputs fraud free (w.r.t the pattern it filters) Event streams. The latter become inputs to the next detector, etc.
We chose the following order based on each pattern's frequency.
![Main job overview](https://github.com/IsraMekki/ClickFraudDetection/blob/master/test_results/CFD.png?raw=true)

## Implemented Detectors
All detectors work the same: each detector takes as input one (or two) streams of Event(eventType, uid, timestamp, impressionId). The suspicious events are logged in specific text files, wheras the clean ones are returned by the detectors as DataStream(s). Note that the intersection between two detectors is not empty
### SuspiciousIpDetector
This pattern is a simple, yet very frequent suspicious behavior. We noticed that 238.186.83.58 is an IP address that comes back quite a lot. We have done some research on it and it seems unassigned (no location is associated to it).
### TooManyClicksDetector
Detects when a user (uid) is associated with too many clicks in a short period of time. 
### ClickBeforeDisplayDetector
Detects when the click event happens before the display event, for the same uid and impressionId (+/- some tolerance threshold). 
### TooManyIpsDetector and TooManyIps2Detector
Detects when a user (uid) is associated to too many IP addresses. We believe the inverse is normal (since there is no port number, the public IP address can be associated to as many users as the LAN can handle, a small LAN can have upp to 254 users). However, when a uid has too many IP addresses, it either means that the user is connected to a lot of devices and each device is connected to a different network (very unlikely for more than 2-3 IPs), or, it's a fraud. We suggest two implementations of this filter: TooManyClicksDetector is a Map-Reduce based filter, and TooManyIps2Detector, inspired by [this](https://ci.apache.org/projects/flink/flink-docs-master/docs/try-flink/datastream/) article, uses KeyedProcessFunctions.
The problem with the second implementation is that it misses fraudulant events. Since the counter of frauds is reset to 0 each time maxIpsPerUser is reached, 1/maxIpsPerUser of the faudulant events are missed.

## CTRCalculator
