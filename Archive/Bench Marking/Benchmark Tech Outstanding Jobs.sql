EXEC Reporting.DropTempTablesFromSession
DECLARE @OutstandingDate AS DATETIME = '2018-07-06 06:00'
DECLARE @ResourceKey AS INT = 8435
DECLARE @BenchaMarkName AS VARCHAR(100) = '20180706_0600_8435'

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
INTO #OutstandingJobsWithCL
FROM #OutstandingJobs oj
UNION
SELECT
	0 GeneID
   ,'Current Location' JobType
   ,ResourceKey
   ,'0' FaultID
   ,'2099-12-31' CalloutDateTime
   ,'0' StoreKey
   ,AVG(Longitude) Longitude
   ,AVG(Latitude) Latitude
   ,'Low' Priority
   ,'2099-12-31' KPITargetDate
   ,0 EstimatedJobDuration
   ,0 HoursToTarget
FROM #OutstandingJobs oj
GROUP BY ResourceKey
UNION
SELECT
	-1 GeneID
   ,'Dummy Job' JobType
   ,ResourceKey
   ,'0' FaultID
   ,'2099-12-31' CalloutDateTime
   ,'0' StoreKey
   ,0 Longitude
   ,0 Latitude
   ,'Low' Priority
   ,'2099-12-31' KPITargetDate
   ,0 EstimatedJobDuration
   ,0 HoursToTarget
FROM #OutstandingJobs oj
GROUP BY ResourceKey

--DELETE FROM [dbo].[OutstandingJobsForProcessing_BenchMarking]
--WHERE ResourceKey = 1630

INSERT INTO [dbo].[OutstandingJobsForProcessing_BenchMarking]
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
	   ,@BenchaMarkName
	FROM #OutstandingJobsWithCL o


--delete outliers
DELETE FROM dbo.[OutstandingJobsForProcessing_BenchMarking]
WHERE ResourceKey = 8435
	AND StoreKey = 5432


DELETE FROM [OutstandingJobsForProcessing_BenchMarking]
WHERE ResourceKey = 8435
	AND StoreKey = 933

UPDATE dbo.OutstandingJobsForProcessing_BenchMarking
SET Latitude = -35.320383
   ,Longitude = 149.133153
WHERE ResourceKey = 8435
AND BenchMarkName = '20180706_0600_8435'
AND JobType = 'Current Location'


--select top 1000 * from [OutstandingJobsForProcessing_BenchMarking] WHERE BenchMarkName = '20180706_0600_8435'








