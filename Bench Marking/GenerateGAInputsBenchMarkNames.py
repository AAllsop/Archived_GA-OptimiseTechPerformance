import pyodbc
import pandas as pd

def main():
    con = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                         "Server=CHADWUA1\DW;"
                         "Database=City_DW;"
                         "Trusted_Connection=yes")
    sql_benchmark_names = "select DISTINCT BenchMarkName from [OutstandingJobsForProcessing_BenchMarking_Iterations]"
    
    benchmark_names = pd.read_sql_query(sql_benchmark_names,con).sort_values(["BenchMarkName"]).reset_index()
    benchmark_names = benchmark_names.drop(columns=["index"])
    
    benchmark_names.to_csv(r"C:\Users\allsopa\OneDrive - City Holdings\Development\Development Tasks\20180701_OptimiseTechPerformance\GIT_OptimiseTechPerformance\Bench Marking\GA Inputs\Benchmark_Names.csv",header=True)