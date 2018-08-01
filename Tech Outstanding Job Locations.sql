DECLARE @ResourceKey INT = 2240
DECLARE @ResourceCapacity INT = 6 --hours
drop table #OutstandingJobsForProcessing
drop table #OutstandingJobs
drop table #Locations
--EXEC Reporting.DropTempTablesFromSession

--initial starting location co-ordinates
DECLARE @ResourceStartLongitude DECIMAL(18,10) = 145.284036
DECLARE @ResourceStartLatitude DECIMAL(18,10) = -37.791168

--get all outstanding jobs
--	with no response
SELECT
	'Response' JobType
   ,ffe.FaultID
   ,ffe.CalloutDate CalloutDateTime
   ,ff.StoreKey
   ,s.Longitude
   ,s.Latitude
   ,ffe.Priority
   ,ffe.ResponseTargetDate KPITargetDate
   ,1 EstimatedJobDuration
INTO #OutstandingJobs
FROM Fault.FactFaultEvent ffe
INNER JOIN Fault.FactFaults ff
	ON ffe.FaultID = ff.FaultID
INNER JOIN Common.Stores s
	ON ff.StoreKey = s.StoreKey
WHERE ffe.Fixed = 0
AND ffe.Completed = 0
AND ff.Cancelled = 0
AND ffe.Cancelled = 0
AND ff.AlarmDateKey > 20180101
AND ff.KPI = 1
AND ff.FirstOnSiteDate IS NULL
AND ffe.ResourceKey = @ResourceKey

--	with no repair
INSERT INTO #OutstandingJobs
	SELECT
		'Repair' JobType
	   ,ffe.FaultID
	   ,ffe.CalloutDate CalloutDateTime
	   ,ff.StoreKey
	   ,s.Longitude
	   ,s.Latitude
	   ,ffe.Priority
	   ,ffe.RepairTargetDate KPITargetDate
	   ,1 EstimatedJobDuration
	FROM Fault.FactFaultEvent ffe
	INNER JOIN Fault.FactFaults ff
		ON ffe.FaultID = ff.FaultID
	INNER JOIN Common.Stores s
		ON ff.StoreKey = s.StoreKey
	WHERE ffe.Fixed = 0
	AND ffe.Completed = 0
	AND ff.Cancelled = 0
	AND ffe.Cancelled = 0
	AND ff.AlarmDateKey > 20180101
	AND ff.KPI = 1
	AND ffe.ResourceKey = @ResourceKey
	AND NOT EXISTS (SELECT
			''
		FROM #OutstandingJobs oj
		WHERE ffe.FaultID = oj.FaultID
		AND ffe.CalloutDate = oj.CalloutDateTime)
		
--SELECT
--	ROW_NUMBER() OVER (ORDER BY oj.JobType) GeneID
--   ,*
--   ,DATEDIFF(HOUR,oj.CalloutDateTime,oj.KPITargetDate) AS HoursToTarget
--INTO #OutstandingJobsForProcessing
--FROM #OutstandingJobs oj

SELECT
	*
INTO #OutstandingJobsForProcessing
FROM dbo.OutstandingJobsForProcessing

DECLARE @ResourceStartLongitude DECIMAL(18,10) = 145.284036
DECLARE @ResourceStartLatitude DECIMAL(18,10) = -37.791168
--get distances between locations
SELECT
	0 AS LocationKey
   ,@ResourceStartLongitude Longitude
   ,@ResourceStartLatitude Latitude
INTO #Locations
UNION ALL
SELECT DISTINCT
	oj.StoreKey
   ,oj.Longitude
   ,oj.Latitude
FROM #OutstandingJobsForProcessing oj


DELETE from #Locations
WHERE Longitude IS NULL

SELECT
	l.LocationKey LocationKeyStartPoint
   ,l1.LocationKey LocationKeyEndPoint
   ,GEOGRAPHY::Point(l.Latitude,l.Longitude,4326).STDistance(GEOGRAPHY::Point(l1.Latitude,l1.Longitude,4326)) AS MetersBetweenPoints
   ,CAST(l.LocationKey AS VARCHAR) + '|' + CAST(l1.LocationKey AS VARCHAR) LocationLookupKey
INTO dbo.LocationDistances
FROM #Locations l
CROSS JOIN #Locations l1




--create population
Declare @PopulationSize = 12
DECLARE @MaxGeneId INT = (SELECT
		MAX(GeneID)
	FROM #OutstandingJobsForProcessing)

CREATE TABLE #Population (
	ChromosomeID INT
   ,GeneID INT
   ,TimeToTargetCost INT
   ,DistanceCost INT
   ,PriorityCost INT
)



select top 1000 * from dbo.LocationDistances ld
drop TABLE dbo.LocationDistances
ADD LocationLookupKey VARCHAR(12)

UPDATE dbo.LocationDistances
SET LocationLookupKey = CAST(LocationKeyStartPoint AS VARCHAR) + '|' + CAST(LocationKeyEndPoint AS VARCHAR)


update dbo.OutstandingJobsForProcessing
SET EstimatedJobDuration = 1

update dbo.OutstandingJobsForProcessing
SET HoursToTarget = 8 WHERE faultid IN (5439073/*,5462030*/)

DELETE FROM dbo.OutstandingJobsForProcessing WHERE GeneID = -1





Declare @Min_Dist DECIMAL(18,4) = (SELECT MIN(MetersBetweenPoints) FROM dbo.LocationDistances WHERE MetersBetweenPoints > 0)
Declare @inv_contraction_rate DECIMAL(18,4) = .03
;
WITH a AS(
select  LocationKeyStartPoint
			   ,LocationKeyEndPoint
			   ,MetersBetweenPoints
			   ,(((MetersBetweenPoints/@Min_Dist-1)*@inv_contraction_rate)*@Min_Dist)+@Min_Dist AS NewDistance
			   ,LocationLookupKey 
			   from dbo.LocationDistances)


Select LocationKeyStartPoint
	  ,LocationKeyEndPoint
	  ,MetersBetweenPoints
	  ,NewDistance/n.MaxNewDistance MetersBetweenPoints_norm
	  ,LocationLookupKey 
	  INTO dbo.LocationDistancesNorm
	  from a			   
	  CROSS Join (SELECT MAX(NewDistance) MaxNewDistance FROM a) n



			   