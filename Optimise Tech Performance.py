#dev issues - search for "dev"
#dev - remove gene cross over types when in prod

import pyodbc
import pandas as pd
from sklearn import preprocessing
import numpy as np
import random

import os

chromosome_capacity = 6 #hours
population_size_limit = 30
mutation_rate = 0.30 #%
elite_size = 10
temp_childdf = []
temp_auditdf1 = []
chromosome_index_no = 0

#component testing versions
crossover_type = "partial" #gene crossover type
parent_selection_type = "random"

env = os.environ["COMPUTERNAME"]
if env != "7-PC" :
    con = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                         "Server=CHADWUA1\DW;"
                         "Database=City_DW;"
                         "Trusted_Connection=yes")
    sql_outstanding_jobs = "Select * from dbo.OutstandingJobsForProcessing"
    outstanding_jobsdf = pd.read_sql_query(sql_outstanding_jobs,con).set_index("GeneID")
    sql_locations = "Select * from dbo.LocationDistancesNorm"
    locations_alldf = pd.read_sql_query(sql_locations,con).set_index("LocationLookupKey")
else:
#if at home import from CSV
    outstanding_jobsdf = pd.read_csv(r"C:\Users\7\Documents\GitHub\work-techoptimisation\OutstandingJobsForProcessing.csv").set_index("GeneID")
    locations_alldf = pd.read_csv(r"C:\Users\7\Documents\GitHub\work-techoptimisation\LocationDistancesNorm.csv").set_index("LocationLookupKey")

# dev - randomise the est job duration
outstanding_jobsdf["EstimatedJobDuration"] = outstanding_jobsdf[outstanding_jobsdf.index != 0].EstimatedJobDuration.apply(lambda x:random.choice([1,1.5,2,2.5,3,3.5]))
outstanding_jobsdf["EstimatedJobDuration"] = outstanding_jobsdf["EstimatedJobDuration"].fillna(0)

#cleanse 
priority_dict = {"High":0,"Medium":0.5,"Low":1,"N/A":0}
outstanding_jobsdf["Priority_norm"] = outstanding_jobsdf["Priority"].map(priority_dict)
#normalise data
min_max_scaler = preprocessing.MinMaxScaler()
x = outstanding_jobsdf[["HoursToTarget"]].astype("float")
x_scaled = min_max_scaler.fit_transform(x)
outstanding_jobsdf["HoursToTarget_norm"] = x_scaled

potential_genes = outstanding_jobsdf.index.tolist()


#import locations

#x = locations_alldf[["MetersBetweenPoints"]].astype(float)           
#x_scaled = min_max_scaler.fit_transform(x)
#locations_alldf["MetersBetweenPoints_norm"] = x_scaled
  
#create lookups
hours_to_target_lookup= outstanding_jobsdf["HoursToTarget_norm"].to_dict()
priority_lookup= outstanding_jobsdf["Priority_norm"].to_dict()

#population functions
def population_additional_columns(pop_df):
    pop_df = populationdf
    pop_df["Size"] = 0    
    global size_column_loc
    size_column_loc= pop_df.columns.get_loc("Size")+1
    
    pop_df["GenerationID"] = 0
    pop_df["RowChanged"] = 0
    pop_df["constraint_workable_hours"] = 0
    pop_df["cost_TimeToKPITarget"] = 0
    pop_df["cost_Priority"] = 0
    pop_df["cost_Distance"] = 0    

def population_costs (pop_df):    
    pop_df = populationdf
    #calculate costs
    chromosome_costs_time_to_target = {}
    chromosome_costs_priority = {}
    chromosome_costs_distance = {}
    chromosome_constraint_workable_hours = {}
    for row in pop_df.itertuples():
        i = row[0]
        s = row[size_column_loc]
        v = list(row[1:s+2])   
        chromosome_costs_time_to_target.update({i:costs_time_to_target(v)})   
        chromosome_costs_priority.update({i:costs_priority(v)})
        chromosome_costs_distance.update({i:job_intra_store_attributes(v,"MetersBetweenPoints_norm")})   
        chromosome_constraint_workable_hours.update({i:constraint_workable_hours(v)})    
    pop_df["cost_TimeToKPITarget"] = pop_df.index.to_series().map(chromosome_costs_time_to_target)
    pop_df["cost_Priority"] = pop_df.index.to_series().map(chromosome_costs_priority)
    pop_df["cost_Distance"] = pop_df.index.to_series().map(chromosome_costs_distance)
    pop_df["cost_Total"] = pop_df["cost_TimeToKPITarget"] + pop_df["cost_Priority"] + pop_df["cost_Distance"]
    pop_df["constraint_workable_hours"] = pop_df.index.to_series().map(chromosome_constraint_workable_hours)

#cost functions
def costs_time_to_target (chromosome):
    r = sum([hours_to_target_lookup[x] for x in chromosome])
    return r
    
def costs_priority (chromosome):
    r = sum([priority_lookup[x] for x in chromosome])
    return r

def costs_distance (chromosome,attribute):
    #convert chromosome to a dataframe
    chromosomedf = pd.DataFrame(chromosome,columns = ["GeneID"])
    #insert last record so the shift operation (later on) works correctly
    chromosomedf = chromosomedf.append(chromosomedf.iloc[-1,0:2],ignore_index=True)
    #get storekey at the job to be undertaken (end point) and the prior location (start point)
    end_pointdf = chromosomedf.merge(outstanding_jobsdf,left_on="GeneID",right_index=True,how="left")[["GeneID","StoreKey"]]
    start_pointdf = end_pointdf.shift(periods=1, axis=0).fillna(0)
    location_distance_loopkupdf = end_pointdf.merge(start_pointdf,left_index=True,right_index=True,how="left")
    location_distance_loopkupdf["StoreKeyLookup"] = location_distance_loopkupdf["StoreKey_y"].astype(int).astype(str) + "|" + location_distance_loopkupdf["StoreKey_x"].astype(str)
    location_distancedf = location_distance_loopkupdf.merge(locations_alldf,left_on="StoreKeyLookup",right_on="LocationLookupKey",how="left").set_index(location_distance_loopkupdf.index)
    location_distancedf.reset_index(inplace=True)
    r = location_distancedf[attribute].sum()
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
#    pop = []
#    chromosome = [0,3,15,18,16,19,2]
#    attribute = "MinutesBetweenPoints"
    #convert chromosome to a dataframe
    chromosomedf = pd.DataFrame(chromosome,columns = ["GeneID"])
    #insert last record so the shift operation (later on) works correctly
    chromosomedf = chromosomedf.append(chromosomedf.iloc[-1,0:2],ignore_index=True)
    #get storekey at the job to be undertaken (end point) and the prior location (start point)
    end_pointdf = chromosomedf.merge(outstanding_jobsdf,left_on="GeneID",right_index=True,how="left")[["GeneID","StoreKey"]]
    start_pointdf = end_pointdf.shift(periods=1, axis=0).fillna(0)
    location_distance_loopkupdf = end_pointdf.merge(start_pointdf,left_index=True,right_index=True,how="left")
    location_distance_loopkupdf["StoreKeyLookup"] = location_distance_loopkupdf["StoreKey_y"].astype(int).astype(str) + "|" + location_distance_loopkupdf["StoreKey_x"].astype(str)
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

def gene_crossover (pop,p):
#    p = [2,5]
#    pop = populationdf
    child_pop = pd.DataFrame(columns=pop_column_headers + ["Size"])
    #get the size of the smallest chromosome - this determines the range of the crossover    
    max_range = min(pop.loc[p,"Size"]) + 1
    crossover_points = [random.randrange(1,max_range),random.randrange(1,max_range)]
    crossover_range = list(range(min(crossover_points),max(crossover_points) + 1))
    #treat a python bug when list contains a single element
    if len(crossover_range) == 1:
        crossover_range = crossover_range[0]
    #loop 2x (to create 2 children)
    x = 0
    while x <= 1: #1: 
        parent_a = p[0]
        parent_b = p[1]
        child_pop = child_pop.append(pop.loc[parent_a,pop_column_headers + ["Size"]].copy())
        #set index for the new child
        new_child_index = get_new_chromosome_index()
        #insert parent into child dataframe
        child_pop = child_pop.rename(index={parent_a:new_child_index})
        #do crossover
        child_pop.loc[new_child_index,crossover_range] = pop.loc[parent_b,crossover_range]
        child_chromosome_size = child_pop.loc[new_child_index,"Size"].astype(int)
        #loop through columns where there are duplicate genes and replace duplicates
        if True in child_pop.loc[new_child_index,list(range(1,child_chromosome_size+1))].duplicated().values:
            i = 1
            while i <= child_chromosome_size-1:
                if child_pop.isnull().loc[new_child_index,i]:
                    proposed_gene = pop.loc[parent_b,i]
                    #if the proposed gene from the crossover exists choose a random one
                    if proposed_gene in child_pop.loc[new_child_index].values:
                        valid_genes = list(set(potential_genes) - set(child_pop.loc[new_child_index,1:max_chromosome_size]))
                        proposed_gene = random.choice(valid_genes)     
                    child_pop.loc[new_child_index,i] = proposed_gene
                i = i + 1
        #reverse parents for next loop
        p.sort(reverse=True)
        x = x + 1
    return child_pop
    
def mutation (pop):
#    pop = populationdf
    pop_to_keep = pop.nsmallest(elite_size,["cost_Total"])
    potential_pop_to_mutate = list(set(pop.index) - set(pop_to_keep.index)) 
#    pop.nlargest(np.floor(population_size_limit * mutation_rate).astype(int),"cost_Total")
    pop_to_mutate = random.sample(potential_pop_to_mutate,np.floor(population_size * mutation_rate).astype(int))
    for mutation_chromosome in pop_to_mutate:
        mutation_gene = random.randrange(1,max_chromosome_size)
        valid_genes = list(set(outstanding_jobsdf.index) - set(pop.iloc[mutation_chromosome,1:max_chromosome_size]) - set([0])) #remove the zero indexed chromosome
        proposed_gene = random.choice(valid_genes)  
        pop.loc[mutation_chromosome,mutation_gene] = proposed_gene
        pop.loc[mutation_chromosome,"Hierachy"] = pop.loc[mutation_chromosome,"Hierachy"] + "m"

def get_new_chromosome_index():
    global chromosome_index_no
    chromosome_index_no = chromosome_index_no + 1
    return chromosome_index_no
        
def repair_chromosome_size (k,v):
    while len(v) <= max_chromosome_size-1:
        v.extend([-1]) #-1 is a dummy job with zero cost

#Create population
max_genes = potential_genes[-1]

chromosome_complete = 0
population_size = 0
population = {}
constraint_travel_time_values= {}
chromosome_index_no = 0
max_chromosome_size = 0
while population_size <= population_size_limit-1:
    #maintain a running total of occupied capacity
    occupied_capacity = 0
    total_travel_time = 0
    chromosome_complete = 0
    chromosome = [0] #zero is a dummy job representing the techs initial start location
    while chromosome_complete <= 0:
        #get a random gene from the available genes
        valid_genes = list(set(potential_genes) - set(chromosome)- set([-1,0]))
        random_gene = random.choice(valid_genes)
        random_gene_volume = outstanding_jobsdf.at[random_gene,"EstimatedJobDuration"]
        occupied_capacity = occupied_capacity + random_gene_volume 
        
        chromosome.append(random_gene)
        if occupied_capacity >= chromosome_capacity:
            chromosome_complete = 1
        random_gene_volume = outstanding_jobsdf.iloc[1:2]    
        
        l = len(chromosome)
        if l > max_chromosome_size:
            max_chromosome_size = l
            
    chromosome_index_no = get_new_chromosome_index()        
    population.update({chromosome_index_no:chromosome})
    
    #get travel time
    travel_time = job_intra_store_attributes(chromosome,"MinutesTravelBetweenPoints")
    constraint_travel_time_values.update({chromosome_index_no:travel_time})    
    population_size = population_size + 1
    

chromosome_sizes = {}    
##make all chromosomes the same size
for k,v in population.items():
    chromosome_sizes.update({k:len(v)-1})
    repair_chromosome_size(k,v)

pop_column_headers = list(range(0,max_chromosome_size))

populationdf = pd.DataFrame({})
populationdf = pd.DataFrame.from_dict(population,orient="index")
population_additional_columns(populationdf)
populationdf["Size"] = populationdf.index.to_series().map(chromosome_sizes)
population_costs(populationdf)
      
#start the evolution
generation = 1    
#initialise audit
audit = pd.DataFrame(columns=["GenerationID","MinCost"])
while generation <= 1:
    breeding = 1
    child_population = {}
    child_populationdf = pd.DataFrame(columns=populationdf.columns)
    #breed the kids
    while breeding <= population_size_limit/2:
        parents = [0,0]
        while min(parents) == max(parents): #ensure no asexual repro
            parents = [select_parent_for_mating(populationdf,3),select_parent_for_mating(populationdf,3)]        
        child_populationdf = child_populationdf.append(gene_crossover(populationdf,parents))
        breeding = breeding + 1 

    #remove the 'size' column and transpose to a list
    child_population = child_populationdf.iloc[:,pop_column_headers].transpose().to_dict("list")
    #get children costs and append to main population dataframe
    population_costs(child_populationdf)
    child_populationdf["GenerationID"] = generation
    child_populationdf["RowChanged"] = 0
#    generation = 2
    populationdf = populationdf.append(child_populationdf)
    
    
    ------------------------------------
    CALL A REPAIR FUNCTION HERE
    ------------------------------------
        
    populationdf = populationdf.nsmallest(population_size,columns="cost_Total")
    populationdf.reset_index(inplace=True,drop=True)
    
#    generation = generation + 1    
    mutation(populationdf)
    
    #recalculate costs for the mutated parents - this is really inefficient, need to revise!!!    
    p = populationdf.iloc[:,0:max_chromosome_size].values.tolist()

    #calculate costs
    chromosome_costs_time_to_target = []
    chromosome_costs_priority = []
    chromosome_costs_distance = []
    
    for row in p:
        costs_time_to_target(chromosome_costs_time_to_target,row)   
        costs_priority(chromosome_costs_priority,row) 
        costs_distance(chromosome_costs_distance,row) 
    
    populationdf["cost_TimeToKPITarget"] = chromosome_costs_time_to_target
    populationdf["cost_Priority"] = chromosome_costs_priority
    populationdf["cost_Distance"] = chromosome_costs_distance
      
    populationdf["cost_Total"] = populationdf["cost_TimeToKPITarget"] + populationdf["cost_Priority"] + populationdf["cost_Distance"]    
        
    audit_record = pd.DataFrame([[generation,min(populationdf["cost_Total"])]], columns=["GenerationID","MinCost"])
    audit = audit.append(audit_record)

    generation = generation + 1    
        

#output for analysis
outstanding_jobsdf.to_csv(r"C:\Users\allsopa\OneDrive - City Holdings\Development\Development Tasks\20180701_OptimiseTechPerformance\Outputs\outstanding_jobsdf.csv")
locations_alldf.to_csv(r"C:\Users\allsopa\OneDrive - City Holdings\Development\Development Tasks\20180701_OptimiseTechPerformance\Outputs\locations_alldf.csv")
populationdf.to_csv(r"C:\Users\allsopa\OneDrive - City Holdings\Development\Development Tasks\20180701_OptimiseTechPerformance\Outputs\populationdf.csv")

#
#
##get the pop record that matches the last min cost
#populationdf = populationdf.reset_index(inplace=True)
#best_solutiondf = populationdf[populationdf["cost_Total"] == audit_record]
#
#print(populationdf.columns)
##
##audit_record
##
#
#
#
#



print (chromosome_index_no)















