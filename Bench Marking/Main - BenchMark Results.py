#External processes to run
#   1. \SQL\Actioned Jobs.sql
#   2. \SQL\Outstanding Jobs.sql

import GenerateGAInputsBenchMarkNames as bm
import GenerateGAInputsOutstandingJobs as oj
import pandas as pd
from pyproj import Geod
from sklearn import preprocessing
import GA as ga
import os.path
from shutil import copyfile

#import pdb
#
#pdb.set_trace()

bm.main()
oj.main()
GA_input_folder = r"C:\Users\allsopa\OneDrive - City Holdings\Development\Development Tasks\20180701_OptimiseTechPerformance\GIT_OptimiseTechPerformance\Bench Marking\GA Inputs" + "\\"
GA_output_folder = r"C:\Users\allsopa\OneDrive - City Holdings\Development\Development Tasks\20180701_OptimiseTechPerformance\GIT_OptimiseTechPerformance\Bench Marking\GA Outputs" + "\\"
best_solutions_filename = GA_output_folder + "best_solutions.csv"

#if os.path.exists(best_solutions_filename):
#    os.remove(best_solutions_filename)

wgs84_geod = Geod(ellps='WGS84') #Distance will be measured on this ellipsoid - more accurate than a spherical method
#Get distance between pairs of lat-lon points
def Distance(lat1,lon1,lat2,lon2):
  az12,az21,dist = wgs84_geod.inv(lon1,lat1,lon2,lat2) 
  return dist

#import all jobs
outstanding_jobsdf = pd.read_csv(GA_input_folder + "Outstanding_Jobs.csv")
#   get job count per done day
outstanding_jobsdf["BenchMarkDay"] = outstanding_jobsdf["BenchMarkName"].apply(lambda x:x[:10])
jobs_by_done_date_count = outstanding_jobsdf.groupby(["BenchMarkDay"])["JobCount"].max().reset_index()

#import all bench mark names
bench_mark_names = pd.read_csv(GA_input_folder + "Benchmark_Names.csv")
bench_mark_names = list(bench_mark_names["BenchMarkName"])
bench_mark_names = bench_mark_names[:18]
#
for bench_mark_name in bench_mark_names:
        
    #bench_mark_name = "2018_07_18_040000"
    #        print(1)
    #    
    #get outstanding jobs
    outstanding_jobs_for_benchmark = outstanding_jobsdf[outstanding_jobsdf["BenchMarkName"]==bench_mark_name]
    #drop jobs that have already been allocated
    if os.path.exists(best_solutions_filename):
        best_solutionsdf = pd.read_csv(best_solutions_filename)
    else:
        best_solutionsdf =pd.DataFrame({"faultid":[-1]}) #-1 is just a placeholder
    allocated_jobs = list(best_solutionsdf["faultid"].drop_duplicates())
    outstanding_jobs_for_benchmark = outstanding_jobs_for_benchmark[~outstanding_jobs_for_benchmark["faultid"].isin(allocated_jobs)] 
    outstanding_jobs_for_benchmark.index = outstanding_jobs_for_benchmark.index.astype(int)
    
    #need to change the current location if the app is being refreshed during the day (e.g. on a high priority callout)
    if "040000" not in bench_mark_name:
        #import current store details
        current_store_details = pd.read_csv(GA_input_folder + "Current_Store_Location.csv")
        current_lat = current_store_details.loc[0,"Latitude"]
        current_lon = current_store_details.loc[0,"Longitude"]
        outstanding_jobs_for_benchmark.loc[outstanding_jobs_for_benchmark["GeneID"] == 0,"Latitude"] = current_lat
        outstanding_jobs_for_benchmark.loc[outstanding_jobs_for_benchmark["GeneID"] == 0,"Longitude"] = current_lon
        
    #Create locations data.
    locationsdf = outstanding_jobs_for_benchmark[["StoreKey","Latitude","Longitude"]].drop_duplicates()
    locationsdf["Key"] = 1
    locations_distances = pd.merge(locationsdf,locationsdf,left_on="Key",right_on="Key",how="inner")
    locations_distances["LookupKey"] = locations_distances["StoreKey_x"].astype(int).astype(str) + "|" + locations_distances["StoreKey_y"].astype(int).astype(str)
    locations_distances["MetersBetweenPoints"] = 0
    for row in locations_distances.itertuples():
        i = row[0]
        slat = row[2]
        slon = row[3]
        elat = row[6]
        elon = row[7]
        dis = Distance(slat,slon,elat,elon)
        locations_distances.loc[i,"MetersBetweenPoints"] = dis
    #normalise distances
    min_max_scalar = preprocessing.MinMaxScaler()        
    x = locations_distances[["MetersBetweenPoints"]].astype("float")
    x_scaled = min_max_scalar.fit_transform(x)
    locations_distances["MetersBetweenPoints_norm"] = x_scaled
    locations_alldf = locations_distances.copy()    
    locations_alldf.index = locations_alldf["LookupKey"]  
    
    #output files for importing into the GA
    outstanding_jobs_for_benchmark.to_csv(GA_input_folder + "Outstanding_Jobs_per_BenchMark.csv",index=False)
    locations_alldf.to_csv(GA_input_folder + "Locations_Per_BenchMark.csv",index=False)
    
    #Run GA
    ga.main( bench_mark_name = bench_mark_name
            ,population_size_limit = 100
            ,convergence_generation = 20
            ,artificial_selection_fraction = 0.1 
            ,artificial_selection_sample_size = 0.25
            ,mutation_rate = 0.10 #%
            ,elite_size = 10
            ,hours_to_target_ignore_threshold = 100
            ,ResourceKey = 1630
            ,population_selection_type = "full random" #[full random, include artifical selection]
            ,crossover_type = "diagonal" #gene crossover type [diagonal,parallel]
            ,parent_selection_type = "random"
            ,termination_type = "no change over n iterations"
            ,termination_type_value = 5)
    
    
    #import best soltuions back in and trim out superfluous jobs
    best_solutions_rollingdf = pd.read_csv(best_solutions_filename)    
    best_solutions_rollingdf ["BenchMarkDay"] = best_solutions_rollingdf ["BenchMarkName"].apply(lambda x:x[:10])
    best_solutions_rollingdf ["J"] = 1
    best_solutions_rollingdf ["RollingJobCount"] = best_solutions_rollingdf .groupby(["BenchMarkDay"])['J'].cumsum()
    best_solutions_rollingdf  = pd.merge(best_solutions_rollingdf ,jobs_by_done_date_count,left_on="BenchMarkDay",right_on="BenchMarkDay",how="left")
    
    best_solutions_rollingdf  = best_solutions_rollingdf [best_solutions_rollingdf ["RollingJobCount"] <= best_solutions_rollingdf ["JobCount"]]
    
    best_solutions_rollingdf  = best_solutions_rollingdf .drop(columns=["J","RollingJobCount","JobCount","BenchMarkDay"])
    best_solutions_rollingdf .to_csv(GA_output_folder + "best_solutions.csv",header=True,index=False)
    
    
    copyfile(GA_output_folder + "best_solutions.csv",GA_output_folder + "best_solutions" + bench_mark_name + ".csv")
        
        
        
        
        
        
        
        
