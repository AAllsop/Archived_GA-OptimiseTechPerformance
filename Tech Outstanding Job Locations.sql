DROP TABLE dbo.LocationDistances
DROP TABLE dbo.LocationDistancesNorm
DROP TABLE dbo.ResourceKey

SELECT 1630 ResourceKey INTO dbo.ResourceKey

EXEC Reporting.DropTempTablesFromSession

--initial starting location co-ordinates
DECLARE @ResourceStartLongitude DECIMAL(18,10) = 153.172883 --145.284036
DECLARE @ResourceStartLatitude DECIMAL(18,10) = -27.66047 ---37.791168


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
FROM dbo.OutstandingJobsForProcessing oj
Inner Join dbo.ResourceKey rk
ON oj.ResourceKey = rk.ResourceKey
WHERE oj.GeneID>0

DELETE FROM #Locations
WHERE Longitude IS NULL

SELECT
	l.LocationKey LocationKeyStartPoint
   ,l1.LocationKey LocationKeyEndPoint
   ,GEOGRAPHY::Point(l.Latitude,l.Longitude,4326).STDistance(GEOGRAPHY::Point(l1.Latitude,l1.Longitude,4326)) AS MetersBetweenPoints
   ,CAST(l.LocationKey AS VARCHAR) + '|' + CAST(l1.LocationKey AS VARCHAR) LocationLookupKey
INTO dbo.LocationDistances
FROM #Locations l
CROSS JOIN #Locations l1

DECLARE @Min_Dist DECIMAL(18,4) = (SELECT
		MIN(MetersBetweenPoints)
	FROM dbo.LocationDistances
	WHERE MetersBetweenPoints > 0)
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
	FROM dbo.LocationDistances)

SELECT
	LocationKeyStartPoint
   ,LocationKeyEndPoint
   ,MetersBetweenPoints
   ,NewDistance / n.MaxNewDistance MetersBetweenPoints_norm
   ,LocationLookupKey
INTO dbo.LocationDistancesNorm
FROM a
CROSS JOIN (SELECT
		MAX(NewDistance) MaxNewDistance
	FROM a) n



UPDATE dbo.LocationDistancesNorm			   
SET MetersBetweenPoints = 0,
	MetersBetweenPoints_norm = 0
WHERE LocationKeyStartPoint = LocationKeyEndPoint
			   