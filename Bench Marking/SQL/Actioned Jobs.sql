drop table dbo.TechDoneJobs

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


Declare @ResourceKey AS INT = 1653
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
WHERE ffe.ResourceKey = @ResourceKey
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
	WHERE ffe.ResourceKey = @ResourceKey
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
	WHERE fp.FirstCallOutEngResourceKey = @ResourceKey
	AND fp.CompletedDateKey = CONVERT(VARCHAR,@d,112)

SET @d = DATEADD(DAY,1,@d)

END

--3641
--1643
--1653
Select 
*, @ResourceKey ResourceKey 
,ROW_NUMBER() OVER (ORDER BY FirstOnSiteDate,RepairTargetDate) RowNo
INTO dbo.TechDoneJobs
from #DoneJobs dj 


DELETE FROM TechDoneJobs WHERE ResourceKey = 1653 AND StoreKey = 3338
