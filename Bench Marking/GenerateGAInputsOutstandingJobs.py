import pyodbc
import pandas as pd

def main():
    con = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                         "Server=CHADWUA1\DW;"
                         "Database=City_DW;"
                         "Trusted_Connection=yes")
    sql = "select * from dbo.[OutstandingJobsForProcessing_BenchMarking_Iterations]"
    
    outstanding_jobs = pd.read_sql_query(sql,con)
    #outstanding_jobs["BenchMarkName"] = outstanding_jobs["BenchMarkName"].apply(lambda x: x.replace(":","").replace("-","_"))
    outstanding_jobs.to_csv(r"C:\Users\allsopa\OneDrive - City Holdings\Development\Development Tasks\20180701_OptimiseTechPerformance\GIT_OptimiseTechPerformance\Bench Marking\GA Inputs\Outstanding_Jobs.csv",header=True)
    
    
    
