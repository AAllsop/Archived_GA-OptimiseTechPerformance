EXEC Reporting.DropTempTablesFromSession
DROP TABLE dbo.LocationDistances_BenchMarking
DROP TABLE dbo.LocationDistancesNorm_BenchMarking

CREATE TABLE dbo.LocationDistancesNorm_BenchMarking (
	LocationKeyStartPoint INT
   ,LocationKeyEndPoint INT
   ,MetersBetweenPoints DECIMAL(10,2)
   ,MetersBetweenPoints_norm DECIMAL(10,2)
   ,LocationLookupKey VARCHAR(20)
   ,ResourceKey INT
   ,BenchMarkName VARCHAR(100)
)


SELECT DISTINCT
	BenchMarkName
   ,0 Processed
INTO #BenchMarksToProcess
FROM [OutstandingJobsForProcessing_BenchMarking_Iterations]

DECLARE @BenchMarkname AS VARCHAR(100) = (SELECT
		MIN(BenchMarkName)
	FROM #BenchMarksToProcess
	WHERE Processed = 0)

WHILE @BenchMarkname IS NOT NULL
BEGIN

DROP TABLE LocationDistances_BenchMarking
drop table #Locations
drop table #ResourcesToProcess

--get distances between locations
SELECT DISTINCT
	oj.StoreKey LocationKey
   ,oj.Longitude
   ,oj.Latitude
   ,oj.ResourceKey
   ,oj.BenchMarkName
INTO #Locations
FROM dbo.[OutstandingJobsForProcessing_BenchMarking_Iterations] oj
WHERE BenchMarkName = @BenchMarkname


DELETE FROM #Locations
WHERE Longitude IS NULL

SELECT
	l.LocationKey LocationKeyStartPoint
   ,l1.LocationKey LocationKeyEndPoint
   ,GEOGRAPHY::Point(l.Latitude,l.Longitude,4326).STDistance(GEOGRAPHY::Point(l1.Latitude,l1.Longitude,4326)) AS MetersBetweenPoints
   ,CAST(l.LocationKey AS VARCHAR) + '|' + CAST(l1.LocationKey AS VARCHAR) LocationLookupKey
   ,l.ResourceKey
INTO dbo.LocationDistances_BenchMarking
FROM #Locations l
CROSS JOIN #Locations l1
WHERE l.ResourceKey = l1.ResourceKey

---loop through and create for each resource
SELECT DISTINCT
	ResourceKey
   ,0 Processed
INTO #ResourcesToProcess
FROM #Locations

DECLARE @ResourceKey AS INT = 0
WHILE @ResourceKey IS NOT NULL
BEGIN

SET @ResourceKey = (SELECT
		MIN(ResourceKey)
	FROM #ResourcesToProcess rtp
	WHERE Processed = 0)

DECLARE @Min_Dist DECIMAL(18,4) = (SELECT
		MIN(MetersBetweenPoints)
	FROM dbo.LocationDistances_BenchMarking
	WHERE MetersBetweenPoints > 0
	AND ResourceKey = @ResourceKey)
DECLARE @inv_contraction_rate DECIMAL(18,4) = .03

;
WITH a
AS
(SELECT
		LocationKeyStartPoint
	   ,LocationKeyEndPoint
	   ,MetersBetweenPoints
	   ,(((MetersBetweenPoints / @Min_Dist - 1) * @inv_contraction_rate) * @Min_Dist) + @Min_Dist AS NewDistance
	   ,LocationLookupKey
	   ,ResourceKey
	FROM dbo.LocationDistances_BenchMarking
	WHERE ResourceKey = @ResourceKey)
	
INSERT INTO dbo.LocationDistancesNorm_BenchMarking
	SELECT
		LocationKeyStartPoint
	   ,LocationKeyEndPoint
	   ,MetersBetweenPoints
	   ,NewDistance / n.MaxNewDistance MetersBetweenPoints_norm
	   ,LocationLookupKey
	   ,ResourceKey
	   ,@BenchMarkname
	FROM a
	CROSS JOIN (SELECT
			MAX(NewDistance) MaxNewDistance
		FROM a) n
	WHERE a.ResourceKey = @ResourceKey

UPDATE dbo.LocationDistancesNorm_BenchMarking
SET MetersBetweenPoints = 0
   ,MetersBetweenPoints_norm = 0
WHERE LocationKeyStartPoint = LocationKeyEndPoint
AND ResourceKey = @ResourceKey
AND BenchMarkName = @BenchMarkname

UPDATE #ResourcesToProcess
SET Processed = 1
WHERE ResourceKey = @ResourceKey
END

UPDATE #BenchMarksToProcess
SET Processed = 1
WHERE BenchMarkName = @BenchMarkname

SET @BenchMarkname = (SELECT
		MIN(BenchMarkName)
	FROM #BenchMarksToProcess
	WHERE Processed = 0)

END





