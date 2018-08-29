#dev issues - search for "dev"
import pandas as pd
from sklearn import preprocessing
import numpy as np
import random
import os.path

GA_input_folder = r"C:\Users\allsopa\OneDrive - City Holdings\Development\Development Tasks\20180701_OptimiseTechPerformance\GIT_OptimiseTechPerformance\Bench Marking\GA Inputs" + "\\"
GA_output_folder = r"C:\Users\allsopa\OneDrive - City Holdings\Development\Development Tasks\20180701_OptimiseTechPerformance\GIT_OptimiseTechPerformance\Bench Marking\GA Outputs" + "\\"

bench_mark_name = "2018_07_13_040000"
population_size_limit = 100
convergence_generation = 20
artificial_selection_fraction = 0.1 
artificial_selection_sample_size = 0.25
mutation_rate = 0.10 #%
elite_size = 10
hours_to_target_ignore_threshold = 100
ResourceKey = 1630
population_selection_type = "full random" #[full random, include artifical selection]
crossover_type = "diagonal" #gene crossover type [diagonal,parallel]
parent_selection_type = "random"
termination_type = "no change over n iterations"
termination_type_value = 5

running_benchmark = True
artificial_selection_limit = np.floor(population_size_limit * artificial_selection_fraction)   
global chromosome_index_no
global best_solution_compiled
chromosome_index_no = 0

#population functions
def population_additional_columns(pop_df):
#    pop_df = populationdf
    pop_df["GenerationID"] = 0
    pop_df["Mutations"] = 0
    pop_df["RowChanged"] = 1 
#    global RowChanged_Pos
#    RowChanged_Pos= pop_df.columns.get_loc("RowChanged")
    pop_df["cost_TimeToKPITarget"] = 0
    pop_df["cost_Priority"] = 0
    pop_df["cost_Distance"] = 0    
    pop_df["cost_Position"] = 0    

def population_costs (pop_df):    
#    pop_df = populationdf
#    pop_df.loc[2416,"RowChanged"] = 1
    #calculate costs
    chromosome_costs_time_to_target = dict(zip(pop_df.index,pop_df["cost_TimeToKPITarget"]))
    chromosome_costs_priority = dict(zip(pop_df.index,pop_df["cost_Priority"]))
    chromosome_costs_distance = dict(zip(pop_df.index,pop_df["cost_Distance"]))
    chromosome_costs_position = dict(zip(pop_df.index,pop_df["cost_Position"]))
    for row in pop_df[pop_df["RowChanged"]==1].itertuples():
        i = row[0]
        v = list(row[2:max_chromosome_size+2])   
        chromosome_costs_time_to_target.update({i:costs_time_to_target(v)})   
        chromosome_costs_priority.update({i:costs_priority(v)})
        chromosome_costs_distance.update({i:job_intra_store_attributes(v,"MetersBetweenPoints_norm")})   
        chromosome_costs_position.update({i:costs_priority_gene_position(v)})   
    pop_df["cost_TimeToKPITarget"] = pop_df.index.to_series().map(chromosome_costs_time_to_target)
    pop_df["cost_Priority"] = pop_df.index.to_series().map(chromosome_costs_priority)
    pop_df["cost_Distance"] = pop_df.index.to_series().map(chromosome_costs_distance)
    pop_df["cost_Position"] = pop_df.index.to_series().map(chromosome_costs_position)
    pop_df["cost_Total"] = pop_df["cost_TimeToKPITarget"] + pop_df["cost_Priority"] + pop_df["cost_Distance"] + pop_df["cost_Position"]
    pop_df["RowChanged"] = 0
    
#cost functions
def costs_time_to_target (chromosome):
    r = sum([hours_to_target_lookup[x] for x in chromosome])
    return r
    
def costs_priority (chromosome):
    r = sum([priority_lookup[x] for x in chromosome])
    return r

def costs_priority_gene_position (chromosome):
#    chromosome = [19,23,24,18,26]
    r = 0
#    if "High" in outstanding_jobsdf[outstanding_jobsdf.index.isin(chromosome)]["Priority"].values:
    priority_genes = pd.DataFrame({"GeneID":chromosome})
    priority_genes = pd.merge(priority_genes,outstanding_jobsdf,left_on = "GeneID", right_index = True, how="inner")[["GeneID","Priority"]]
    priority_genes = pd.merge(priority_genes,positions_cost_lookup,left_index=True,right_index=True, how="inner")
    priority_genes.loc[priority_genes["Priority"] != "High","cost_norm"] = 0
    r = sum(priority_genes["cost_norm"])
    return r        

def constraint_travel_time (chromosome):
#    pop = {}
#    pop_index = 3
#    chromosome = [0,13,21]
    tt_storekeys = outstanding_jobsdf.loc[chromosome,:]["StoreKey"]
    for tt_i,storekey in enumerate(tt_storekeys):
        tt_value = 0
        if tt_i != 0:
            tt_lookup_key = tt_storekeys[tt_storekeys.index[tt_i-1]].astype(str) + "|" + str(storekey)
            tt_value = tt_value + locations_alldf.at[tt_lookup_key,"MinutesBetweenPoints"]/60 #dev - make this field to hours to save having to convert       
    return tt_value
#    pop.update({pop_index:tt_value})

def constraint_workable_hours (chromosome):
#    chromosome = [0,13,21]    
    hrs = sum(outstanding_jobsdf.loc[chromosome,"EstimatedJobDuration"])
    return hrs

def job_intra_store_attributes (chromosome,attribute):
#    chromosome = [23,12]
#    attribute = "MetersBetweenPoints_norm"
    #convert chromosome to a dataframe
    chromosomedf = pd.DataFrame(chromosome,columns = ["GeneIDs"])
    #insert last record so the shift operation (later on) works correctly
    chromosomedf = chromosomedf.append(chromosomedf.iloc[-1,0:2],ignore_index=True)
    #get storekey at the job to be undertaken (end point) and the prior location (start point)
    end_pointdf = chromosomedf.merge(outstanding_jobsdf,left_on="GeneIDs",right_index=True,how="left")[["GeneIDs","StoreKey"]]
    start_pointdf = end_pointdf.shift(periods=1, axis=0).fillna(0)
    location_distance_loopkupdf = end_pointdf.merge(start_pointdf,left_index=True,right_index=True,how="left")
    location_distance_loopkupdf["StoreKeyLookup"] = location_distance_loopkupdf["StoreKey_y"].astype(int).astype(str) + "|" + location_distance_loopkupdf["StoreKey_x"].astype(int).astype(str)
    location_distancedf = location_distance_loopkupdf.merge(locations_alldf,left_on="StoreKeyLookup",right_index =True,how="left").set_index(location_distance_loopkupdf.index)
    location_distancedf.reset_index(inplace=True)
    attribute_value = location_distancedf[attribute].sum()
    return attribute_value

def select_parent_for_mating (pop,k):
    if parent_selection_type == "random":
        potential_parents = {}
        i = 1
        while i <= k:   
            member = random.choice(pop.index)
            member_cost = pop.at[member,"cost_Total"]
            potential_parents[member] = member_cost
            parent = min(potential_parents,key=potential_parents.get)
            i = i + 1
            return parent;

def gene_crossover_parallel (pop,p):
#    p = [279,295]
#    pop = populationdf
    child_pop = pd.DataFrame(columns=pop_column_headers)
    crossover_points = [random.randrange(1,max_chromosome_size),random.randrange(1,max_chromosome_size)]
    crossover_range = list(range(min(crossover_points),max(crossover_points) + 1))
    #treat a python bug when list contains a single element
    if len(crossover_range) == 1:
        crossover_range = crossover_range[0]
    #loop 2x (to create 2 children)
    x = 0
    while x <= 1: #1: 
        parent_a = p[0]
        parent_b = p[1]
        child_pop = child_pop.append(pop.loc[parent_a,pop_column_headers].copy())
        #set index for the new child
        new_child_index = get_new_chromosome_index()
        #insert parent into child dataframe
        child_pop = child_pop.rename(index={parent_a:new_child_index})
        #do crossover
        child_pop.loc[new_child_index,crossover_range] = pop.loc[parent_b,crossover_range]
        #loop through columns where there are duplicate genes and replace duplicates
        if True in child_pop.loc[new_child_index,pop_column_headers].duplicated().values:
            i = 1
            dup_genes = pd.DataFrame(child_pop.loc[new_child_index,pop_column_headers].duplicated().values)
            dup_genes = dup_genes[dup_genes[0]==True]
            for row in dup_genes.itertuples():
                i = row[0]
                valid_genes = list(set(potential_genes) - set(child_pop.loc[new_child_index,1:max_chromosome_size]))
                proposed_gene = random.choice(valid_genes)    
                child_pop.loc[new_child_index,i] = proposed_gene
        #reverse parents for next loop
        p.sort(reverse=True)
        x = x + 1
    return child_pop
    
def gene_crossover_diagonal (pop,p):
#    p = [1,2]
#    pop = populationdf
    global crossover_target_range
    global crossover_range
    global crossover_len
    global crossover_target
    global t
    global s
    global result
    global z
    child_pop = pd.DataFrame(columns=pop_column_headers)
    crossover_points = [random.randrange(1,max_chromosome_size),random.randrange(1,max_chromosome_size)]
    crossover_range = list(range(min(crossover_points),max(crossover_points) + 1))
    #get crossover target 
    crossover_len = len(crossover_range)
    crossover_target = random.randrange(1,max_chromosome_size)
    #determine where the crossover can be done in the target chromosome
    z = random.choice([-1,1])
    if z == -1:
        result = crossover_target - crossover_len
        if result < 1:
            crossover_target_range = [1,crossover_len]
        else:
            crossover_target_range = [crossover_target-crossover_len+1,crossover_target]
    if z == 1:
        result = crossover_target + crossover_len
        if result > max_chromosome_size:
            crossover_target_range = [max_chromosome_size-crossover_len+1,max_chromosome_size]
        else:
            crossover_target_range = [crossover_target, crossover_target+crossover_len-1]            
    crossover_target_range = list(range(min(crossover_target_range),max(crossover_target_range)+1))
    #treat a python bug when list contains a single element
    crossover_range_len = len(crossover_range)
    if crossover_range_len == 1:
        crossover_range = crossover_range[0]
        crossover_target_range = crossover_target_range[0]
    #loop 2x (to create 2 children)
    x = 0
    while x <= 1: #1: 
        parent_a = p[0]
        parent_b = p[1]
        child_pop = child_pop.append(pop.loc[parent_a,pop_column_headers].copy())
        #set index for the new child
        new_child_index = get_new_chromosome_index()
        #insert parent into child dataframe
        child_pop = child_pop.rename(index={parent_a:new_child_index})
        #do crossover
        if crossover_range_len == 1:
            t = pop.loc[parent_b,crossover_range]
            s = child_pop.loc[new_child_index,crossover_target_range]
        else:
            t = list(pop.loc[parent_b,crossover_range])
            s = list(child_pop.loc[new_child_index,crossover_target_range])
        child_pop.at[new_child_index,crossover_target_range] = t
        #loop through columns where there are duplicate genes and replace duplicates
        dup_values = child_pop.loc[new_child_index,pop_column_headers].duplicated().values 
        if True in dup_values:
            dup_genes = pd.DataFrame(dup_values)
            dup_genes = dup_genes[dup_genes[0]==True]
            for row in dup_genes.itertuples():
                i = row[0]
                valid_genes = list(set(potential_genes) - set(child_pop.loc[new_child_index,pop_column_headers]))
                proposed_gene = random.choice(valid_genes)    
                child_pop.loc[new_child_index,i] = proposed_gene                
        #reverse parents for next loop
        p.sort(reverse=True)
        x = x + 1
    return child_pop

def mutation (pop):
#    pop = populationdf
    pop_to_keep = pop.nsmallest(elite_size,["cost_Total"])
    potential_pop_to_mutate = list(set(pop.index) - set(pop_to_keep.index)) 
    pop_to_mutate = random.sample(potential_pop_to_mutate,np.floor(population_size * mutation_rate).astype(int))
    for mutation_chromosome in pop_to_mutate:
        mutation_gene = random.randrange(1,max_chromosome_size)
        valid_genes = list(set(potential_genes) - set(pop.loc[mutation_chromosome,pop_column_headers]))
        proposed_gene = random.choice(valid_genes)  
        pop.loc[mutation_chromosome,mutation_gene] = proposed_gene
        pop.loc[mutation_chromosome,"RowChanged"] = 1
        pop.loc[mutation_chromosome,"Mutations"] = pop.loc[mutation_chromosome,"Mutations"] + 1

def get_new_chromosome_index():
    global chromosome_index_no
    chromosome_index_no = chromosome_index_no + 1
    return chromosome_index_no

def check_convergence():
    r = False
    generational_sameness = len(audit[audit["MinCost"]==audit_record.iloc[0,1]]["MinCost"])
    if generational_sameness >= termination_type_value:
        r = True
    return r

#------------------------------------------------------------------------------
#Analysis
def compile_best_solution(sol_index):
    global best_solution_compiled
#    sol_index = 22
    best_solutiondf = populationdf[populationdf.index == sol_index]
    best_solution_jobsdf = best_solutiondf.iloc[0,pop_column_headers].to_frame()
    best_solution_jobsdf = best_solution_jobsdf.rename(columns={best_solution_jobsdf.columns[0]:"BestJobID"})
    best_solution_jobsdf["BestJobIDOrdering"] = best_solution_jobsdf.index
    #get distance between stores
    best_solution_jobsdf = pd.merge(best_solution_jobsdf,outstanding_jobsdf,left_on="BestJobID",right_index=True,how="inner")[["BestJobIDOrdering","BestJobID","StoreKey","BenchMarkName","KPITargetDate"]]
    from datetime import datetime 
    best_solution_jobsdf["BenchMarkDate"] = best_solution_jobsdf["BenchMarkName"].apply(lambda x:datetime.strptime(x[:10] + " 00:00:01","%Y_%m_%d %H:%M:%S"))
    best_solution_jobsdf["KPITargetDate"] = best_solution_jobsdf["KPITargetDate"].apply(lambda x:datetime.strptime(x, "%Y-%m-%d %H:%M:%S.%f"))
    best_solution_jobsdf["KPIAchievedDueToModel"] = best_solution_jobsdf[["BenchMarkDate","KPITargetDate"]].apply(KPI_achieved,axis=1)
    start_pointdf = best_solution_jobsdf["StoreKey"].shift(periods=1).fillna(0).to_frame()
    best_solution_jobsdf = pd.merge(best_solution_jobsdf,start_pointdf,left_index=True,right_index=True,how="inner")
    best_solution_jobsdf["LocationLookup"] = best_solution_jobsdf["StoreKey_x"].astype(int).astype(str) + "|" + best_solution_jobsdf["StoreKey_y"].astype(int).astype(str)
    best_solution_jobsdf = pd.merge(best_solution_jobsdf,locations_alldf,left_on="LocationLookup",right_index=True,how="inner")
    outstanding_jobsdf["GeneID"] = outstanding_jobsdf.index
    d = pd.merge(outstanding_jobsdf,best_solution_jobsdf,left_on = "GeneID", right_on = "BestJobID", how="left")
    d["BestJobIDOrdering"].fillna(1000,inplace=True)
    d["KMsBetweenPoints"] = (d[d["BestJobIDOrdering"] < 1000]["MetersBetweenPoints"]/1000)
    best_solution_compiled = d[["BestJobIDOrdering","GeneID","StoreKey","KMsBetweenPoints", "Priority","HoursToTarget","faultid","KPIAchievedDueToModel","BenchMarkDate","KPITargetDate_x"]].sort_values(["BestJobIDOrdering","HoursToTarget"])
    best_solution_compiled["BenchMarkName"] = bench_mark_name
    
def KPI_achieved(x):
    r = 0
    if x["BenchMarkDate"] <= x ["KPITargetDate"]:
        r = 1
    return r
#------------------------------------------------------------------------------
    
#import data
outstanding_jobsdf = pd.read_csv(GA_input_folder + "Outstanding_Jobs_per_BenchMark.csv")
outstanding_jobsdf.index = outstanding_jobsdf["GeneID"]
locations_alldf = pd.read_csv(GA_input_folder  + "Locations_Per_BenchMark.csv")
locations_alldf.index = locations_alldf["LookupKey"]

chromosome_capacity = min(outstanding_jobsdf["JobCount"]) #hours
if chromosome_capacity == 0 :
    chromosome_capacity=1
outstanding_jobsdf["EstimatedJobDuration"] = outstanding_jobsdf["EstimatedJobDuration"].fillna(0)

#cleanse 
priority_dict = {"High":0,"Medium":0.5,"Low":1,"N/A":0}
outstanding_jobsdf["Priority_norm"] = outstanding_jobsdf["Priority"].map(priority_dict)
#set all jobs older than 100hrs to 100hrs. Therefore reducing their influence
non_premature_outstanding_jobs = outstanding_jobsdf[outstanding_jobsdf["HoursToTarget"]<=hours_to_target_ignore_threshold]["EstimatedJobDuration"].sum()
if non_premature_outstanding_jobs > chromosome_capacity*3:
    #reduce the HoursToTarget 
    outstanding_jobsdf.loc[outstanding_jobsdf["HoursToTarget"]>=hours_to_target_ignore_threshold,"HoursToTarget"] = hours_to_target_ignore_threshold

#normalise data
min_max_scaler = preprocessing.MinMaxScaler()
x = outstanding_jobsdf[["HoursToTarget"]].astype("float")
x_scaled = min_max_scaler.fit_transform(x)
outstanding_jobsdf["HoursToTarget_norm"] = x_scaled

potential_genes = list(set(outstanding_jobsdf.index.tolist()) - set([0,-1]))  

#find the potentially largest chromosome size
outstanding_jobs_by_duration = outstanding_jobsdf.loc[:,"EstimatedJobDuration"].sort_values().to_frame()
outstanding_jobs_by_duration["RollingDurationSum"] = outstanding_jobs_by_duration.rolling(window = 10000, min_periods = 1 ).sum()
outstanding_jobs_by_duration = outstanding_jobs_by_duration.reset_index()
outstanding_jobs_over_hrs = outstanding_jobs_by_duration[outstanding_jobs_by_duration["RollingDurationSum"] >= chromosome_capacity].index

if running_benchmark == True:
    if len(potential_genes) < chromosome_capacity:
        max_chromosome_size = len(potential_genes)
    else:    
        max_chromosome_size = chromosome_capacity  
else:
    if len(potential_genes) < chromosome_capacity:
        max_chromosome_size = len(potential_genes)
    else:    
        max_chromosome_size = min(outstanding_jobs_over_hrs)

pop_column_headers = list(range(0,max_chromosome_size+1))

#create a lookup of nomalised positional costs
positions_cost_lookup = pd.DataFrame({"position":range(1,max_chromosome_size+1)})
positions_cost_lookup["inverse cost"] = 1/(np.log(positions_cost_lookup["position"]+1))
min_pos_cost = min(positions_cost_lookup["inverse cost"])
max_pos_cost = max(positions_cost_lookup["inverse cost"])
positions_cost_lookup["cost_norm"] = ((positions_cost_lookup["inverse cost"]-min_pos_cost)/(max_pos_cost-min_pos_cost))*-1

#create lookups
hours_to_target_lookup= outstanding_jobsdf["HoursToTarget_norm"].to_dict()
priority_lookup= outstanding_jobsdf["Priority_norm"].to_dict()

#Create population-------------------------------------------------------------
chromosome_complete = 0
population_size = 0
population = {}

if population_selection_type == "include artifical selection":
    while population_size <= artificial_selection_limit-1:
#            occupied_capacity = 0
        chromosome_complete = 0
        chromosome = [0] #zero is a dummy job representing the techs initial start location
        #create artificial population - i.e. a population of possible elites. 
        #If the obj is to reduce time to target cost then might as well start population with a mixture of lowest cost genes
        outstanding_jobs_count = outstanding_jobsdf[outstanding_jobsdf.index>0].iloc[:,1].count()
        artificial_selection_sample_size = max(artificial_selection_sample_size,max_chromosome_size/outstanding_jobs_count)
        artificial_populationdf = outstanding_jobsdf[outstanding_jobsdf.index>0].nsmallest(np.floor(artificial_selection_sample_size*outstanding_jobs_count).astype(int),columns="HoursToTarget")
        potential_artificial_genes = set(artificial_populationdf.index)
        while chromosome_complete <= max_chromosome_size-1:
            #get a random gene from the available genes
            valid_genes = list(potential_artificial_genes - set(chromosome))#- set([-1,0]))
            random_gene = random.choice(valid_genes)
            chromosome.append(random_gene)
            chromosome_complete = chromosome_complete + 1    
        chromosome_index_no = get_new_chromosome_index()        
        population.update({chromosome_index_no:chromosome})
        population_size = population_size + 1

while population_size <= population_size_limit-1:
    #maintain a running totalx of occupied capacity
#        occupied_capacity = 0
    chromosome_complete = 0
    chromosome = [0] #zero is a dummy job representing the techs initial start location
    while chromosome_complete <= max_chromosome_size-1:
        #get a random gene from the available genes
        if not len(list(set(potential_genes) - set(chromosome))):
            chromosome_complete = max_chromosome_size+1
        else:
            valid_genes = list(set(potential_genes) - set(chromosome))       
        random_gene = random.choice(valid_genes)
        chromosome.append(random_gene)
        chromosome_complete = chromosome_complete + 1    
    chromosome_index_no = get_new_chromosome_index()        
    population.update({chromosome_index_no:chromosome})
    population_size = population_size + 1

populationdf = pd.DataFrame({})
populationdf = pd.DataFrame.from_dict(population,orient="index")
population_additional_columns(populationdf)
population_costs(populationdf)
     

#start the evolution-----------------------------------------------------------
generation = 1    
#initialise audit
audit = pd.DataFrame(columns=["GenerationID","MinCost"])
while generation <= convergence_generation:
    breeding = 1
    if max_chromosome_size > 1 and len(potential_genes) > max_chromosome_size: # only breed or mutate if enough potential genes exist
        child_populationdf = pd.DataFrame(columns=populationdf.columns)
        #breed the kids
        while breeding <= population_size_limit/2:
            parents = [0,0]
            parents = [select_parent_for_mating(populationdf,3),select_parent_for_mating(populationdf,3)]   
            if crossover_type == "parallel":
                child_populationdf = child_populationdf.append(gene_crossover_parallel(populationdf,parents))
            elif crossover_type == "diagonal":
                child_populationdf = child_populationdf.append(gene_crossover_diagonal(populationdf,parents))
            breeding = breeding + 1 
        #get children costs and append to main population dataframe
        child_populationdf["RowChanged"] = 1
        child_populationdf["GenerationID"] = generation
        child_populationdf["Mutations"] = 0
        population_costs(child_populationdf)
        populationdf = populationdf.append(child_populationdf)
        populationdf = populationdf.nsmallest(population_size,columns="cost_Total")
        
        mutation(populationdf)
    
    #recalculate costs for the mutated chromosomes
    population_costs(populationdf)

    audit_record = pd.DataFrame([[generation,min(populationdf["cost_Total"])]], columns=["GenerationID","MinCost"])
    audit = audit.append(audit_record)
    generation = generation + 1    

    #check convergence
    if check_convergence() == True:
        generation = convergence_generation + 1

##get the pop record that matches the last min cost
#populationdf = populationdf.reset_index(inplace=True)
best_solutionsdf = populationdf[populationdf["cost_Total"] == audit_record.loc[0,"MinCost"]]
best_solution_index = best_solutionsdf.index[0]
compile_best_solution(best_solution_index)

best_solution_compiled = best_solution_compiled[(best_solution_compiled["BestJobIDOrdering"]<1000) & (best_solution_compiled["StoreKey"]!=0)]
best_solutions_filename = GA_output_folder + "best_solutions.csv"

if not os.path.exists(best_solutions_filename):
    best_solution_compiled[best_solution_compiled["GeneID"]==-1].to_csv(best_solutions_filename,header=True,index=False)
best_solution_compiled.to_csv(best_solutions_filename,header=False,mode="a",index=False)

#get the last store that was in the techs optimised list as this'll be the last store he should be at when the list is refreshed
current_storekey = best_solution_compiled.iloc[-1,2].astype(int)
current_lat = min(outstanding_jobsdf[outstanding_jobsdf["StoreKey"]==current_storekey]["Latitude"])
current_lon = min(outstanding_jobsdf[outstanding_jobsdf["StoreKey"]==current_storekey]["Longitude"])

current_store_location = pd.DataFrame({"StoreKey":[current_storekey],"Latitude":[current_lat],"Longitude":[current_lon]})
current_store_location.to_csv(GA_input_folder + "Current_Store_Location.csv",index=False)











