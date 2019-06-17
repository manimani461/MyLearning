


WITH temp as (
SELECT
    * FROM [mobileapps-qcpr-ios-instructor-stream-Input] partition by EventName
)

SELECT
[EventId],
[EventName],
[SessionId],
[Compression Score],
[Average Compression Depth],
[Average Compression Rate],
[Average Number of Compressions per Cycle],
[Percent adequate Depth],
[Percent adequate Rate],
[EventProcessedUtcTime],
[PartitionId],
[EventEnqueuedUtcTime]
INTO
   [Instructor-score-compressions]
FROM
    temp 
    where EventName LIKE '%Page - Instructor Score compressions%'


SELECT
[EventId],
[EventName],
[SessionId],
[Ventilation Score],
[Average Volume],
[Ventilations Per Cycle Statistics],
[Max Number Of Ventilations In Cycle],
[Min Number Of Ventilations In Cycle],
[Number Of Ventilation Cycles],
[Number Of Ventilations],
[PercentAdequateDepth],
[Percent Adequate Rate],
[Percentage Adequate Volume],
[Percentage Too High Volume],
[Percentage Too Low Volume],
[Percent Compressions Too Fast],
[Percent Compressions Too Slow],
[Percent Compressions With Complete Release],
[Percent Compressions With Correct HandPosition],
[Percent Hands On Time],
[Percent TooDeepDepth],
[Percent TooShallowDepth],
[Standard DeviationOnVolume],
[Standard DeviationCompressionRate],
[Number Of Compression Cycles],
[EventProcessedUtcTime],
[PartitionId],
[EventEnqueuedUtcTime]
INTO
   [Instructor-score-ventilations]
FROM
   temp 
   where EventName LIKE '%Page - Instructor Score ventilations%'


SELECT
[EventId],
[EventName],
[SessionId],
[Hint For Improvement],
[Total Time],
[Total Score],
[Average No Flow Time],
[EventProcessedUtcTime],
[PartitionId],
[EventEnqueuedUtcTime]
INTO
   [Instructor-score-various]
FROM
    temp
    where EventName LIKE '%Page - Instructor Score various%'
	

SELECT
[EventId],
[EventName],
[SessionId],
[Session Type],
[Session Duration],
[Number of Manikins Connected],
[Connected Manikins Types],
[RaceMode],
[Previous page],
[EventProcessedUtcTime],
[PartitionId],
[EventEnqueuedUtcTime]	
INTO
   [Instructor-Session-Collection-View]
FROM
    temp
    where EventName LIKE '%Page: Session Collection View%'

SELECT
[EventId],
[EventName],
[SessionId],
[device name],
[Serial Number],
[Firmware revision],
[NewFirmwareUpgradeAvailable],
[Manikin Type],
[Supports15CompressionsMode],
[EventProcessedUtcTime],
[PartitionId],
[EventEnqueuedUtcTime]
INTO
   [Instructor-device-connected]
FROM
    temp
    where EventName LIKE '%Instructor device connected%'

SELECT
[EventId],
[EventName],
[SessionId],
[CompressionsCounter],
[VentilationsCounter],
[BatteryChangeCounter],
[CriticalBatteryLevelCounter],
[RuntimeBatteriesMinutes],
[RuntimeAdapterMinutes],
[PowerOnCounter],
[Skillguide.RuntimeMinutes],
[Skillguide.ButtonPressCounter],
[CurrentRawRestingValue-2],
[CurrentRawRestingValue-8],
[EventProcessedUtcTime],
[PartitionId],
[EventEnqueuedUtcTime]
INTO
   [LifetimeData]
FROM
    temp
    where EventName LIKE '%LifetimeData%'

