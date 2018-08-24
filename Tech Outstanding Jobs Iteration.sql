DROP TABLE #AppRefreshDateTimes
;
WITH InitialDatesToProcess
AS
(SELECT
		CAST(DoneDate AS DATETIME) + CAST('04:00' AS DATETIME) InitialDateTime
	FROM (SELECT DISTINCT
			DoneDate
		FROM dbo.TechDoneJobs) i)


SELECT
	tdj.CalloutDate AppRefreshDateTime
   ,0 Processed
INTO #AppRefreshDateTimes
FROM dbo.TechDoneJobs tdj
INNER JOIN InitialDatesToProcess
	ON tdj.DoneDate = CAST(InitialDateTime AS DATE)
		AND CAST(tdj.CalloutDate AS DATE) = CAST(InitialDateTime AS DATE)
UNION
SELECT
	InitialDateTime
   ,0
FROM InitialDatesToProcess
ORDER BY 1


DECLARE @ResourceKey AS INT = 1653
DECLARE @OutstandingDate AS DATETIME = (SELECT
		MIN(AppRefreshDateTime)
	FROM #AppRefreshDateTimes
	WHERE Processed = 0)

WHILE @OutstandingDate IS NOT NULL
BEGIN
SET @OutstandingDate = (SELECT
		MIN(AppRefreshDateTime)
	FROM #AppRefreshDateTimes
	WHERE Processed = 0)

DROP TABLE #OutstandingJobs
DROP TABLE #OutstandingJobsWithCL
DECLARE @BenchMarkName AS VARCHAR(100) = CONVERT(VARCHAR,@OutstandingDate,120)

--get all outstanding jobs on the morning
--	with no response
SELECT
	'Response' JobType
   ,ffe.ResourceKey
   ,ffe.faultid
   ,ffe.CalloutDate CalloutDateTime
   ,ffe.FirstOnSiteDate
   ,ff.FixedEODate
   ,s.Latitude
   ,s.Longitude
   ,ff.StoreKey
   ,ffe.Priority
   ,ffe.ResponseTargetDate KPITargetDate
   ,1 EstimatedJobDuration
   ,ffe.ResponseAchieved KPIAchieved
INTO #OutstandingJobs
FROM Fault.FactFaultEvent ffe
INNER JOIN Fault.FactFaults ff
	ON ffe.faultid = ff.faultid
INNER JOIN Common.Stores s
	ON ff.StoreKey = s.StoreKey
WHERE @OutstandingDate BETWEEN ffe.CalloutDate AND ISNULL(ffe.FirstOnSiteDate,@OutstandingDate)  --OR CAST(ffe.CalloutDate AS DATE) = @OutstandingDate)
AND ff.Cancelled = 0
AND ffe.Cancelled = 0
--AND ff.KPI = 1
AND ffe.ResourceKey = @ResourceKey
ORDER BY ffe.FirstOnSiteDate


--	with no repair
INSERT INTO #OutstandingJobs
	SELECT
		'Repair' JobType
	   ,ffe.ResourceKey
	   ,ffe.faultid
	   ,ffe.CalloutDate CalloutDateTime
	   ,ffe.FirstOnSiteDate
	   ,ffe.FixedEODate
	   ,s.Latitude
	   ,s.Longitude
	   ,ff.StoreKey
	   ,ffe.Priority
	   ,ffe.RepairTargetDate KPITargetDate
	   ,1 EstimatedJobDuration
	   ,ffe.RepairEOAchieved
	FROM Fault.FactFaultEvent ffe
	INNER JOIN Fault.FactFaults ff
		ON ffe.faultid = ff.faultid
	INNER JOIN Common.Stores s
		ON ff.StoreKey = s.StoreKey
	WHERE @OutstandingDate BETWEEN ffe.CalloutDate AND ISNULL(ffe.FixedEODate,@OutstandingDate)
	AND ff.Cancelled = 0
	AND ffe.Cancelled = 0
	AND ffe.ResourceKey = @ResourceKey
	--AND ff.KPI = 1
	AND NOT EXISTS (SELECT
			''
		FROM #OutstandingJobs oj
		WHERE ffe.faultid = oj.faultid
		AND ffe.CalloutDate = oj.CalloutDateTime)

--outstanding PPMs
INSERT INTO #OutstandingJobs
	SELECT
		'PPM'
	   ,@ResourceKey
	   ,fp.PPM_ID
	   ,fp.ReleaseDate
	   ,fp.CompletedDate
	   ,NULL
	   ,s.Latitude
	   ,s.Longitude
	   ,fp.StoreKey
	   ,'Low'
	   ,fp.TargetCompletionDate
	   ,1 EstimatedJobDuration
	   ,CompletedOnTime KPIAchieved
	FROM Fault.FactPPM fp
	INNER JOIN Common.Stores s
		ON fp.StoreKey = s.StoreKey
	WHERE @OutstandingDate BETWEEN fp.ReleaseDate AND ISNULL(fp.CompletedDate,@OutstandingDate)
	AND fp.FirstCallOutEngResourceKey = @ResourceKey
	AND fp.TargetCompletionDate >= DATEADD(M,DATEDIFF(M,0,@OutstandingDate),0)



SELECT
	ROW_NUMBER() OVER (PARTITION BY oj.ResourceKey ORDER BY oj.JobType) GeneID
   ,JobType
   ,ResourceKey
   ,faultid
   ,CalloutDateTime
   ,StoreKey
   ,Longitude
   ,Latitude
   ,Priority
   ,KPITargetDate
   ,EstimatedJobDuration
   ,DATEDIFF(HOUR,oj.CalloutDateTime,oj.KPITargetDate) AS HoursToTarget
   ,KPIAchieved
INTO #OutstandingJobsWithCL
FROM #OutstandingJobs oj
UNION
SELECT
	0 GeneID
   ,'Current Location' JobType
   ,ResourceKey
   ,'0' faultid
   ,'2099-12-31' CalloutDateTime
   ,'0' StoreKey
   ,AVG(Longitude) Longitude
   ,AVG(Latitude) Latitude
   ,'Low' Priority
   ,'2099-12-31' KPITargetDate
   ,0 EstimatedJobDuration
   ,0 HoursToTarget
   ,0 KPIAchieved
FROM #OutstandingJobs oj
GROUP BY ResourceKey
UNION
SELECT
	-1 GeneID
   ,'Dummy Job' JobType
   ,ResourceKey
   ,'0' faultid
   ,'2099-12-31' CalloutDateTime
   ,'0' StoreKey
   ,0 Longitude
   ,0 Latitude
   ,'Low' Priority
   ,'2099-12-31' KPITargetDate
   ,0 EstimatedJobDuration
   ,0 HoursToTarget
   ,0 KPIAchieved
FROM #OutstandingJobs oj
GROUP BY ResourceKey

INSERT INTO [dbo].[OutstandingJobsForProcessing_BenchMarking_Iterations]
	SELECT
		GeneID
	   ,JobType
	   ,ResourceKey
	   ,faultid
	   ,CalloutDateTime
	   ,StoreKey
	   ,Longitude
	   ,Latitude
	   ,Priority
	   ,KPITargetDate
	   ,EstimatedJobDuration
	   ,HoursToTarget
	   ,@BenchMarkName
	   ,KPIAchieved
	FROM #OutstandingJobsWithCL o

UPDATE #AppRefreshDateTimes
SET Processed = 1
WHERE AppRefreshDateTime = @OutstandingDate

END




--UPDATE dbo.OutstandingJobsForProcessing_BenchMarking
--SET Latitude = -35.320383
--   ,Longitude = 149.133153
--WHERE ResourceKey = 8435
--AND BenchMarkName = '20180706_0600_8435'
--AND JobType = 'Current Location'


--select top 1000 * from [OutstandingJobsForProcessing_BenchMarking] WHERE BenchMarkName = '20180706_0600_8435'


--SELECT TOP 1000
--	*
--FROM #OutstandingJobs
--ORDER BY 4





SELECT TOP 1000
	*
FROM [dbo].[OutstandingJobsForProcessing_BenchMarking_Iterations]

