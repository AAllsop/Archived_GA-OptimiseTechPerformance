import pyodbc
import pandas as pd


con = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                     "Server=CHADWUA1\DW;"
                     "Database=City_DW;"
                     "Trusted_Connection=yes")
sql_locations = "Select * from dbo.LocationDistancesNorm_BenchMarking"
locations_alldf = pd.read_sql_query(sql_locations,con).set_index("LocationLookupKey")
sql_tech_done_jobs = ";WITH a AS "\
                      "  (SELECT DISTINCT "\
                      "  		CAST(LEFT(ojfpbmi.BenchMarkName,10) AS DATE) DoneDate "\
                      "  	   ,ojfpbmi.BenchMarkName "\
                      "  	   ,ojfpbmi.Latitude "\
                      "  	   ,ojfpbmi.Longitude "\
                      "  	FROM dbo.OutstandingJobsForProcessing_BenchMarking_Iterations ojfpbmi "\
                      "  	WHERE JobType = 'Current Location' "\
                      "  	AND BenchMarkName LIKE '%04:00:00%') "\
                      " SELECT	* FROM dbo.TechDoneJobs tdj "\
                      " UNION SELECT DoneDate,'Current Location','Current Location','0',DoneDate,NULL,NULL,Latitude,Longitude,0,'Low',NULL,NULL,0,0,0 FROM a"
tech_done_jobsdf = pd.read_sql_query(sql_tech_done_jobs,con)

#calculate distances
tech_done_jobs_start_point = tech_done_jobsdf.shift(periods=1,axis=0).fillna(0)
location_points_lookup = pd.merge(tech_done_jobsdf,tech_done_jobs_start_point,left_index=True,right_index=True,how="inner")
location_points_lookup = location_points_lookup.loc[:,["StoreKey_y","StoreKey_x"]]
location_points_lookup["LookupKey"] = location_points_lookup.loc[:,"StoreKey_y"].astype(int).astype(str) + "|" + location_points_lookup.loc[:,"StoreKey_x"].astype(int).astype(str)

d = pd.merge(location_points_lookup,locations_alldf,left_on = "LookupKey",right_index=True,how="inner")