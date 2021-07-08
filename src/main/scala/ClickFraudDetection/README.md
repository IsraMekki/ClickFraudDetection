## Overview of the main Job
The following figure depicts our naive pipeline for dealing with the problem. We designed the main job so as it uses the implemented detectors one after another. Each detector takes as input a stream containing fraudulent Events and outputs fraud free (w.r.t the pattern it filters) Event streams. The latter become inputs to the next detector, etc.
The order of detectors was chosen based on each pattern's frequency.
![Main job overview](https://github.com/IsraMekki/ClickFraudDetection/blob/master/test_results/CFD.png?raw=true)

This architecture however has some serious issues:
* One needs to be very careful while filtering "clean" events. Some events might be lost due to windowing and reduce operations, which can seriously alter the rest of the pipeline. Another risk is to have some events duplicated, which is not something we want.
* Although we thought the arcitecture was simple, it is, in fact, complex since components need to interact with each other. When one detector goes bad, it is quite difficult to track things down and find the problem.
* The use of sliding windows in some detectors might have been useful to detect more frauds. However, with this pipeline, we ended up with duplicates which was bad for our analysis.
* We did not know if we needed to filter both clicks and displays or clicks only. For sake of simplicity, we opted for the second alternative.
* We also noticed that the ClickBeforeDisplay detector does nothing. This pattern is redundent with the first one (suspiciousIp), so we dicided to discard it.

This resulted in the following pipeline. It still needs to be improved, but it has better performance then the naive one.
![Main job overview](https://github.com/IsraMekki/ClickFraudDetection/blob/master/test_results/CFD_new.png?raw=true)



## Implemented Detectors
All detectors work the same: each detector takes as input one (or two) streams of Event(eventType, uid, timestamp, impressionId). The suspicious events are logged in specific text files, wheras the clean ones are returned by the detectors as DataStream(s). Note that the intersection between two detectors is not empty.
### SuspiciousIpDetector
This pattern is a simple, yet very frequent suspicious behavior. We noticed that 238.186.83.58 is an IP address that comes back quite a lot. We have done some research on it and it seems unassigned (no location is associated to it).
### TooManyClicksDetector
Detects when a user (uid) is associated with too many clicks in a short period of time. 
### ClickBeforeDisplayDetector
Detects when the click event happens before the display event, for the same uid and impressionId (+/- some tolerance threshold). 
### TooManyIpsDetector and TooManyIps2Detector
Detects when a user (uid) is associated to too many IP addresses. We believe the inverse is normal (since there is no port number, the public IP address can be associated to as many users as the LAN can handle, a small LAN can have up to 254 users). However, when a uid has too many IP addresses, it either means that the user is connected to a lot of devices and each device is connected to a different network (very unlikely for more than 2-3 IPs), or, it's a fraud. We suggest two implementations of this filter: TooManyClicksDetector is a Map-Reduce based filter, and TooManyIps2Detector, inspired by [this](https://ci.apache.org/projects/flink/flink-docs-master/docs/try-flink/datastream/) article, uses KeyedProcessFunctions.
**NB.** we did not adapt the second implementation to our pipeline, it simply streams fraudulent events.

## CTRCalculator
Takes as input two strams (clicks and displays) and calculates the CTR by UID (coud be adapted to other dimensions but we stuck with UID)
