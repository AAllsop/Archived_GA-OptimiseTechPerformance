

--identify those jobs responded to on a specific date
SELECT TOP 1000
	ff.faultid
   ,ffe.CalloutDate
   ,ffe.FirstOnSiteDate
   ,ffe.FixedEODate
   ,s.Latitude
   ,s.Longitude
   ,ff.StoreKey
   ,ffe.Priority
   ,ffe.ResponseTargetDate
   ,ffe.RepairTargetDate
FROM Fault.FactFaultEvent ffe
INNER JOIN Fault.FactFaults ff
	ON ffe.faultid = ff.faultid
INNER JOIN Common.Stores s
	ON ff.StoreKey = s.StoreKey
WHERE ffe.ResourceKey = 5936
AND (CAST(ffe.FirstOnSiteDate AS DATE) BETWEEN '2018-07-01' AND '2018-07-31'
OR CAST(ffe.FixedEODate AS DATE) BETWEEN '2018-07-01' AND '2018-07-31')
ORDER BY ffe.FirstOnSiteDate


SELECT TOP 1000
	*
FROM Fault.FactPPM fp
WHERE fp.FirstCallOutEngResourceKey = 1643
AND LEFT(fp.ScheduledDateKey,6) = 201807

SELECT
	COUNT(*)
   ,ffe.Priority
   ,CAST(ffe.FirstOnSiteDate AS DATE) FirstOnSiteDate

FROM Fault.FactFaultEvent ffe
INNER JOIN Fault.FactFaults ff
	ON ffe.faultid = ff.faultid
INNER JOIN Common.Stores s
	ON ff.StoreKey = s.StoreKey
WHERE ffe.ResourceKey = 1643
AND (CAST(ffe.FirstOnSiteDate AS DATE) BETWEEN '2018-07-01' AND '2018-07-31'
OR CAST(ffe.FixedEODate AS DATE) BETWEEN '2018-07-01' AND '2018-07-31')
GROUP BY CAST(ffe.FirstOnSiteDate AS DATE)
		,ffe.Priority

ORDER BY CAST(ffe.FirstOnSiteDate AS DATE)



EXEC Reporting.DropTempTablesFromSession
--identify those jobs responded to on a specific date
SELECT TOP 0
	'1900-01-01' DoneDate
   ,'Fault' JobType
   ,'Response' KPIType
   ,ff.faultid JobID
   ,ffe.CalloutDate
   ,ffe.FirstOnSiteDate
   ,ffe.FixedEODate
   ,s.Latitude
   ,s.Longitude
   ,ff.StoreKey
   ,ffe.Priority
   ,ffe.ResponseTargetDate
   ,ffe.RepairTargetDate
   ,ffe.ResponseAchieved KPIAchieved
INTO #DoneJobs
FROM Fault.FactFaultEvent ffe
INNER JOIN Fault.FactFaults ff
	ON ffe.faultid = ff.faultid
INNER JOIN Common.Stores s
	ON ff.StoreKey = s.StoreKey
WHERE ffe.ResourceKey = 1643
AND CAST(ffe.FirstOnSiteDate AS DATE) = '1900-01-01'



DECLARE @d AS DATE = '2018-07-01'
WHILE @d <= '2018-07-30'
BEGIN

--identify those jobs responded to on a specific date
INSERT INTO #DoneJobs
SELECT
	@d DoneDate
   ,'Fault' JobType
   ,'Response' KPIType
   ,ff.faultid JobID
   ,ffe.CalloutDate
   ,ffe.FirstOnSiteDate
   ,ffe.FixedEODate
   ,s.Latitude
   ,s.Longitude
   ,ff.StoreKey
   ,ffe.Priority
   ,ffe.ResponseTargetDate
   ,ffe.RepairTargetDate
   ,ffe.ResponseAchieved KPIAchieved
FROM Fault.FactFaultEvent ffe
INNER JOIN Fault.FactFaults ff
	ON ffe.faultid = ff.faultid
INNER JOIN Common.Stores s
	ON ff.StoreKey = s.StoreKey
WHERE ffe.ResourceKey = 1653
AND CAST(ffe.FirstOnSiteDate AS DATE) = @d

INSERT INTO #DoneJobs
	--identify those jobs repaied on a specific date
	SELECT
		@d DoneDate
	   ,'Fault' JobType
	   ,'Repair' KPIType
	   ,ff.faultid JobID
	   ,ffe.CalloutDate
	   ,ffe.FirstOnSiteDate
	   ,ffe.FixedEODate
	   ,s.Latitude
	   ,s.Longitude
	   ,ff.StoreKey
	   ,ffe.Priority
	   ,ffe.ResponseTargetDate
	   ,ffe.RepairTargetDate
	   ,ffe.ResponseAchieved KPIAchieved
	FROM Fault.FactFaultEvent ffe
	INNER JOIN Fault.FactFaults ff
		ON ffe.faultid = ff.faultid
	INNER JOIN Common.Stores s
		ON ff.StoreKey = s.StoreKey
	WHERE ffe.ResourceKey = 1653
	AND CAST(ffe.FixedEODate AS DATE) = @d
	AND NOT EXISTS (SELECT
			''
		FROM #DoneJobs dj
		WHERE dj.JobID = ff.faultid
		AND dj.KPIType = 'Response'
		AND CAST(dj.FirstOnSiteDate AS DATE) = CAST(ISNULL(dj.FixedEODate,'1900-01-01') AS DATE))


INSERT INTO #DoneJobs
	SELECT
		@d DoneDate
	   ,'PPM'
	   ,'PPM'
	   ,fp.PPM_ID
	   ,fp.ReleaseDate
	   ,fp.CompletedDate
	   ,NULL
	   ,s.Latitude
	   ,s.Longitude
	   ,fp.StoreKey
	   ,'Low'
	   ,fp.TargetCompletionDate
	   ,NULL
	   ,CompletedOnTime KPIAchieved
	FROM Fault.FactPPM fp
	INNER JOIN Common.Stores s
		ON fp.StoreKey = s.StoreKey
	WHERE fp.FirstCallOutEngResourceKey = 1653
	AND fp.CompletedDateKey = CONVERT(VARCHAR,@d,112)

SET @d = DATEADD(DAY,1,@d)

END

--3641
--1643
--1653
Select * from #DoneJobs dj 
order by dj.DoneDate,dj.FirstOnSiteDate









