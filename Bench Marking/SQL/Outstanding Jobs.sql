DECLARE @ResourceKey AS INT = (SELECT MAX(tdj.ResourceKey) FROM dbo.TechDoneJobs tdj)

DROP TABLE #AppRefreshDateTimes
DROP TABLE #AppRefreshDateTimesAndJobCounts
TRUNCATE TABLE [dbo].[OutstandingJobsForProcessing_BenchMarking_Iterations]

Declare @FirstDoneDateTime AS DATETIME = (SELECT MIN(DoneDate) FROM TechDoneJobs)
Declare @LastDoneDateTime AS DATETIME = (SELECT MAX(DoneDate) FROM TechDoneJobs)
;
WITH InitialDatesToProcess
AS
(SELECT
		CAST(DoneDate AS DATETIME) + CAST('04:00' AS DATETIME) InitialDateTime
	FROM (SELECT DISTINCT
			DoneDate
		FROM TechDoneJobs) i)

--App to get refreshed at 4am each mornign the tech worked and whenever a high priority job is raised
SELECT
	tdj.CalloutDate AppRefreshDateTime
   ,0 Processed
   ,1 WakeUpDateTime
INTO #AppRefreshDateTimes
FROM dbo.TechDoneJobs tdj
WHERE tdj.CalloutDate BETWEEN @FirstDoneDateTime AND @LastDoneDateTime
UNION
SELECT
	InitialDateTime
   ,0
   ,0
FROM InitialDatesToProcess
ORDER BY 1

--get the number of jobs done (so as to calc how many he an do) after each iteration
SELECT
	*
   ,(SELECT
			COUNT(*)
		FROM dbo.TechDoneJobs t
		WHERE CAST(t.DoneDate AS DATE) = CAST(a.AppRefreshDateTime AS DATE)) 
	JobCount
INTO #AppRefreshDateTimesAndJobCounts
FROM #AppRefreshDateTimes a


DECLARE @OutstandingDate AS DATETIME = (SELECT
		MIN(AppRefreshDateTime)
	FROM #AppRefreshDateTimes
	WHERE Processed = 0)
DECLARE @JobCount AS INT
DECLARE @BenchMarkName AS VARCHAR(100)
DECLARE @CL_Lat AS FLOAT
DECLARE @CL_Lon AS FLOAT

WHILE @OutstandingDate IS NOT NULL
BEGIN
SET @OutstandingDate = (SELECT
		MIN(AppRefreshDateTime)
	FROM #AppRefreshDateTimesAndJobCounts
	WHERE Processed = 0)

SET @JobCount = (SELECT
		(JobCount)
	FROM #AppRefreshDateTimesAndJobCounts
	WHERE AppRefreshDateTime = @OutstandingDate)

DROP TABLE #OutstandingJobs
DROP TABLE #OutstandingJobsWithCL
SET @BenchMarkName = REPLACE(REPLACE(REPLACE(REPLACE(CONVERT(VARCHAR,@OutstandingDate,120),'-','_'),':',''),'.000',''),' ' ,'_')

--get the geo-coordinates for the called out job
;
WITH a
AS
(SELECT
		MAX(tdj.RowNo) RowNo
	FROM dbo.TechDoneJobs tdj
	WHERE tdj.DoneDate = CAST(@OutstandingDate AS DATE)
	AND
	CASE
		WHEN KPIType IN ('Response','PPM') THEN tdj.FirstOnSiteDate
		ELSE tdj.FixedEODate
	END <= @OutstandingDate)

SELECT
	@CL_Lat = tdj.Latitude
   ,@CL_Lon = tdj.Longitude
FROM dbo.TechDoneJobs tdj
INNER JOIN a
	ON tdj.RowNo = a.RowNo

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
WHERE @OutstandingDate >= ffe.CalloutDate --AND ISNULL(ffe.FirstOnSiteDate,@OutstandingDate)  --OR CAST(ffe.CalloutDate AS DATE) = @OutstandingDate)
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
	WHERE @OutstandingDate >= ffe.CalloutDate --AND ISNULL(ffe.FixedEODate,@OutstandingDate)
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
	WHERE @OutstandingDate >= fp.ReleaseDate --AND ISNULL(fp.CompletedDate,@OutstandingDate)
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
   ,DATEDIFF(HOUR,@OutstandingDate,oj.KPITargetDate) AS HoursToTarget
   ,KPIAchieved
INTO #OutstandingJobsWithCL
FROM #OutstandingJobs oj
UNION
--insert a starting location for the day
SELECT
	0 GeneID
   ,'Current Location' JobType
   ,ResourceKey
   ,'0' faultid
   ,'2099-12-31' CalloutDateTime
   ,'0' StoreKey
   ,146.739 Longitude
   ,-19.2885 Latitude
   ,'Low' Priority
   ,'2099-12-31' KPITargetDate
   ,0 EstimatedJobDuration
   ,0 HoursToTarget
   ,0 KPIAchieved
FROM #OutstandingJobs oj
WHERE CAST(@OutstandingDate AS TIME) = '04:00:00.0000000'
GROUP BY ResourceKey

UNION
--insert a starting location for the called out job
--(although this will be overwritten later)
SELECT
	0 GeneID
   ,'Current Location' JobType
   ,ResourceKey
   ,'0' faultid
   ,'2099-12-31' CalloutDateTime
   ,'0' StoreKey
   ,@CL_Lon Longitude
   ,@CL_Lat Latitude
   ,'Low' Priority
   ,'2099-12-31' KPITargetDate
   ,0 EstimatedJobDuration
   ,0 HoursToTarget
   ,0 KPIAchieved
FROM #OutstandingJobs oj
WHERE CAST(@OutstandingDate AS TIME) <> '04:00:00.0000000'
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
	   ,@JobCount
	FROM #OutstandingJobsWithCL o

UPDATE #AppRefreshDateTimesAndJobCounts
SET Processed = 1
WHERE AppRefreshDateTime = @OutstandingDate

END

DELETE o
from dbo.[OutstandingJobsForProcessing_BenchMarking_Iterations] o
WHERE not exists (select '' from dbo.TechDoneJobs tdj where o.faultid = tdj.JobID)
AND JobType <> 'Current Location'

DELETE FROM [OutstandingJobsForProcessing_BenchMarking_Iterations]
WHERE ResourceKey = 1653 AND StoreKey = 3338 


