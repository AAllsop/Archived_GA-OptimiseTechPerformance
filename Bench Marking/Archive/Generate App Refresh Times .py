import pyodbc 
import pandas as pd
from datetime import timedelta

con = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                     "Server=CHADWUA1\DW;"
                     "Database=City_DW;"
                     "Trusted_Connection=yes")
sql_tech_done_jobs = "Select CAST(tdj.DoneDate AS DATETIME) DoneDate,tdj.JobType,tdj.KPIType,tdj.JobID,tdj.CalloutDate,tdj.FirstOnSiteDate,tdj.FixedEODate "\
                	    ",tdj.Latitude,tdj.Longitude,tdj.StoreKey,tdj.Priority,tdj.ResponseTargetDate,tdj.RepairTargetDate,tdj.KPIAchieved,tdj.ResourceKey,tdj.RowNo "\
                     "from dbo.TechDoneJobs tdj"
tech_done_jobsdf = pd.read_sql_query(sql_tech_done_jobs,con)

#morning refresh
tech_done_jobs1 = tech_done_jobsdf["DoneDate"].drop_duplicates().to_frame()
tech_done_jobs1["RefreshDateTime"] = tech_done_jobs1["DoneDate"] + timedelta(hours=4)

#refresh againfor high priority jobs
tech_done_jobs2 = tech_done_jobsdf[tech_done_jobsdf["Priority"]=="High"]
tech_done_jobs2 = tech_done_jobs2[["DoneDate","CalloutDate"]].drop_duplicates()
tech_done_jobs2 = tech_done_jobs2.rename(columns={"CalloutDate":"RefreshDateTime"})

app_refresh_times = pd.concat([tech_done_jobs1,tech_done_jobs2],axis=0)

app_refresh_times.to_csv(r"C:\Users\allsopa\OneDrive - City Holdings\Development\Development Tasks\20180701_OptimiseTechPerformance\GIT_OptimiseTechPerformance\Outputs\app_refresh_times.csv",header=True)



