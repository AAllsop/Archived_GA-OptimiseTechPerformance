DROP TABLE #AppRefreshDateTimes
drop table #AppRefreshDateTimesAndJobCounts
truncate table [dbo].[OutstandingJobsForProcessing_BenchMarking_Iterations]
;
WITH InitialDatesToProcess
AS
(SELECT
		CAST(DoneDate AS DATETIME) + CAST('04:00' AS DATETIME) InitialDateTime
	FROM (SELECT DISTINCT
			DoneDate
		FROM dbo.TechDoneJobs) i)

--select top 1000 * from dbo.TechDoneJobs


SELECT
	tdj.CalloutDate AppRefreshDateTime
   ,0 Processed
INTO #AppRefreshDateTimes
FROM dbo.TechDoneJobs tdj
INNER JOIN InitialDatesToProcess
	ON CAST(tdj.CalloutDate AS DATE) = CAST(InitialDateTime AS DATE)
UNION
SELECT
	InitialDateTime
   ,0
FROM InitialDatesToProcess
ORDER BY 1
--get the number of jobs done (so as to calc how many he an do) after each iteration
SELECT 
	*
   ,(SELECT
			COUNT(*)
		FROM dbo.TechDoneJobs t
		WHERE CAST(t.CalloutDate AS DATE) = CAST(a.AppRefreshDateTime AS DATE)
		AND t.CalloutDate >= a.AppRefreshDateTime) JobCount
INTO #AppRefreshDateTimesAndJobCounts
FROM #AppRefreshDateTimes a



DECLARE @ResourceKey AS INT = 1653
DECLARE @OutstandingDate AS DATETIME = (SELECT
		MIN(AppRefreshDateTime)
	FROM #AppRefreshDateTimes
	WHERE Processed = 0)
Declare @JobCount AS INT
DECLARE @BenchMarkName AS VARCHAR(100)
Declare @CL_Lat AS FLOAT
Declare @CL_Lon AS FLOAT

WHILE @OutstandingDate is not null 
BEGIN
SET @OutstandingDate = (SELECT
		MIN(AppRefreshDateTime)
	FROM #AppRefreshDateTimesAndJobCounts
	WHERE Processed = 0)

SET @JobCount = (SELECT (JobCount) FROM #AppRefreshDateTimesAndJobCounts WHERE AppRefreshDateTime = @OutstandingDate)

DROP TABLE #OutstandingJobs
DROP TABLE #OutstandingJobsWithCL
SET @BenchMarkName = CONVERT(VARCHAR,@OutstandingDate,120)

--get the geo-coordinates for the called out job
; WITH a AS(
select MAX(tdj.RowNo) RowNo from dbo.TechDoneJobs tdj WHERE tdj.DoneDate = CAST(@OutstandingDate AS DATE)
AND CASE WHEN KPITYpe IN ('Response','PPM') then tdj.FirstOnSiteDate ELSE tdj.FixedEODate END <= @OutstandingDate)

select @CL_Lat = tdj.Latitude,@CL_Lon= tdj.Longitude from dbo.TechDoneJobs tdj 
Inner Join a
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



--insert a starting lcoation for each day
; WITH cl AS (select CAST(AppRefreshDateTime AS DATE), MIN( AppRefreshDateTime) from #AppRefreshDateTimesAndJobCounts
group by CAST(AppRefreshDateTime AS DATE))



SELECT  * FROM  #AppRefreshDateTimesAndJobCounts




select top 1000 * from dbo.TechDoneJobs tdj WHERE tdj.DoneDate = '2018-07-03'
order by tdj.FirstOnSiteDate


--

; WITH a AS(
select MAX(tdj.RowNo) RowNo from dbo.TechDoneJobs tdj WHERE tdj.DoneDate = '2018-07-03'
AND CASE WHEN KPITYpe IN ('Response','PPM') then tdj.FirstOnSiteDate ELSE tdj.FixedEODate END <= '2018-07-03 16:29:00.000')

select tdj.Latitude,tdj.Longitude from dbo.TechDoneJobs tdj 
Inner Join a
ON tdj.RowNo = a.RowNo
		





SELECT TOP 1000
	*
FROM [dbo].[OutstandingJobsForProcessing_BenchMarking_Iterations]


WHERE BenchMarkName = '2018-07-03 04:00:00'
order by JobType,faultid

select top 1000 * from [dbo].[OutstandingJobsForProcessing_BenchMarking_Iterations]

select top 1000 * from dbo.TechDoneJobs tdj
WHERE DoneDate = '2018-07-03' order by tdj.FirstOnSiteDate


; WITH a AS (
SELECT tdj.DoneDate, MIN(tdj.FirstOnSiteDate) FirstOnSiteDate from dbo.TechDoneJobs tdj
group by tdj.DoneDate)

select a.DoneDate,tdj.StoreKey,tdj.Latitude,tdj.Longitude from dbo.TechDoneJobs tdj
Inner Join a ON tdj.FirstOnSiteDate = a.FirstOnSiteDate


select top 1000 * from dbo.LocationDistancesNorm_BenchMarking WHERE LocationLookupKey = '2969|2969'