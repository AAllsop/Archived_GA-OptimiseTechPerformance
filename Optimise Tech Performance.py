import pyodbc
import pandas as pd
from sklearn import preprocessing
import numpy as np
import random

chromosome_capacity = 6 #hours
population_size_limit = 300
mutation_rate = 0.30 #%
elite_size = 10
temp_childdf = []
temp_auditdf1 = []

con = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                     "Server=CHADWUA1\DW;"
                     "Database=City_DW;"
                     "Trusted_Connection=yes")

#import job data
sql_outstanding_jobs = "Select * from dbo.OutstandingJobsForProcessing"
outstanding_jobsdf = pd.read_sql_query(sql_outstanding_jobs,con).set_index("GeneID")
#cleanse 
priority_dict = {"High":0,"Medium":0.5,"Low":1,"N/A":0}
outstanding_jobsdf["Priority_norm"] = outstanding_jobsdf["Priority"].map(priority_dict)
#normalise data
min_max_scaler = preprocessing.MinMaxScaler()
x = outstanding_jobsdf[["HoursToTarget"]].astype("float")
x_scaled = min_max_scaler.fit_transform(x)
outstanding_jobsdf["HoursToTarget_norm"] = x_scaled

#import locations
sql_locations = "Select * from dbo.LocationDistancesNorm"
locations_alldf = pd.read_sql_query(sql_locations,con).set_index("LocationLookupKey")
#x = locations_alldf[["MetersBetweenPoints"]].astype(float)           
#x_scaled = min_max_scaler.fit_transform(x)
#locations_alldf["MetersBetweenPoints_norm"] = x_scaled
  
#create lookups
hours_to_target_lookup= outstanding_jobsdf["HoursToTarget_norm"].to_dict()
priority_lookup= outstanding_jobsdf["Priority_norm"].to_dict()

#cost functions
def costs_time_to_target (pop,chromosome):
    pop.append(sum([hours_to_target_lookup[x] for x in chromosome]))
    
def costs_priority (pop,chromosome):
    pop.append(sum([priority_lookup[x] for x in chromosome]))

def costs_distance (pop,chromosome):
#    pop = []
#    chromosome = [0,3,15,18,16,19,2]
        
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
    pop = pop.append(location_distancedf["MetersBetweenPoints_norm"].sum())


#Create population
potential_genes = outstanding_jobsdf.index.tolist()
max_genes = potential_genes[-1]

chromosome_complete = 0
population_size = 0
population = []

while population_size <= population_size_limit-1:
    #maintain a running total of occupied capacity
    occupied_capacity = 0
    total_travel_time = 0
    chromosome_complete = 0
    chromosome = [0] #zero is a dummy job representing the techs initial start location
    while chromosome_complete <= 0:
        #get a random gene from the available genes
        valid_genes = list(set(potential_genes) - set(chromosome))
        random_gene = random.choice(valid_genes)
    
        random_gene_volume = outstanding_jobsdf.at[random_gene,"EstimatedJobDuration"]
               
        #get the last store in the list
        last_job_storekey = outstanding_jobsdf.at[chromosome[-1],"StoreKey"].astype(str)
        this_job_storekey = outstanding_jobsdf.at[random_gene,"StoreKey"].astype(str)
        travel_time = locations_alldf.at[last_job_storekey + "|" + this_job_storekey,"MinutesTravelBetweenPoints"]

        total_travel_time = total_travel_time + travel_time
        occupied_capacity = occupied_capacity + random_gene_volume + total_travel_time
        
        chromosome.append(random_gene)
        if occupied_capacity >= chromosome_capacity:
            chromosome_complete = 1
        random_gene_volume = outstanding_jobsdf.iloc[1:2]    
        
    population.append(chromosome)
    population_size = len(population)
    
#make all chromosomes the same size
max_chromosome_size = max(map(len,population))    
for row in population:
    while len(row) <= max_chromosome_size-1:
        row.extend([-1]) #-1 is a dummy job with zero cost

populationdf = pd.DataFrame({})
populationdf = populationdf.append(population)
populationdf["GenerationID"] = 0
populationdf["hierachy"] = "p"

#add in cost columns
populationdf["cost_TimeToKPITarget"] = 0
populationdf["cost_Priority"] = 0
populationdf["cost_Distance"] = 0
    
#calculate costs
chromosome_costs_time_to_target = []
chromosome_costs_priority = []
chromosome_costs_distance = []

for row in population:
    costs_time_to_target(chromosome_costs_time_to_target,row)   
    costs_priority(chromosome_costs_priority,row) 
    costs_distance(chromosome_costs_distance,row) 

populationdf["cost_TimeToKPITarget"] = chromosome_costs_time_to_target
populationdf["cost_Priority"] = chromosome_costs_priority
populationdf["cost_Distance"] = chromosome_costs_distance
  
populationdf["cost_Total"] = populationdf["cost_TimeToKPITarget"] + populationdf["cost_Priority"] + populationdf["cost_Distance"]


def select_parent_for_mating (pop,k):
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
    childrendf = pd.DataFrame(columns=list(range(0,max_chromosome_size)))
    crossover_points = [random.randrange(1,max_chromosome_size),random.randrange(1,max_chromosome_size)]
    crossover_start_pt = min(crossover_points)
    crossover_end_pt = max(crossover_points) + 1
    #loop 2x (to create 2 children)
    x = 0
    p.sort()
    while x <= 1: #1: 
        parent_a = p[0]
        parent_b = p[1] 
        childrendf = childrendf.append(pop.iloc[parent_a,crossover_start_pt:crossover_end_pt])
        childrendf.loc[parent_a,0] = 0
        i = 1
        #loop through columns
        while i <= max_chromosome_size-1:
            if childrendf.isnull().loc[parent_a,i]:
                proposed_gene = pop.loc[parent_b,i]
                #if the proposed gene from the crossover exists choose a random one
                if proposed_gene in childrendf.loc[parent_a].values:
                    valid_genes = list(set(pop.iloc[parent_b,1:max_chromosome_size]) - set(childrendf.loc[parent_a,1:max_chromosome_size]))
                    proposed_gene = random.choice(valid_genes)     
                    
#                    print(generation,parent_a,parent_b,i,proposed_gene)
                    
                childrendf.loc[parent_a,i] = proposed_gene
            i = i + 1
        p.sort(reverse=True)
        x = x + 1
    children = childrendf.values.tolist()
    
    return children
        
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
        pop.loc[mutation_chromosome,"hierachy"] = pop.loc[mutation_chromosome,"hierachy"] + "m"

      
#start the evolution
generation = 0    
audit = pd.DataFrame(columns=["GenerationID","MinCost"])
tempauditdf = []
while generation <= 0:
    breeding = 1
    child_population = []
    child_populationdf = pd.DataFrame({})
    while breeding <= population_size_limit/2:
        parents = [0,0]
        while min(parents) == max(parents):
            parents = [select_parent_for_mating(populationdf,3),select_parent_for_mating(populationdf,3)]        
    
        child_population.extend(gene_crossover(populationdf,parents))
        
        breeding = breeding + 1 
    
    child_populationdf = child_populationdf.append(child_population)
    child_populationdf.reset_index(inplace=True,drop=True)
    child_populationdf["GenerationID"] = generation
    child_populationdf["hierachy"] = "c"
    
    #add in cost columns
    child_populationdf["cost_TimeToKPITarget"] = 0
    child_populationdf["cost_Priority"] = 0
    child_populationdf["cost_Distance"] = 0
    
    #calculate costs
    chromosome_costs_time_to_target = []
    chromosome_costs_priority = []
    chromosome_costs_distance = []
    
    for row in child_population:
        costs_time_to_target(chromosome_costs_time_to_target,row)   
        costs_priority(chromosome_costs_priority,row) 
        costs_distance(chromosome_costs_distance,row) 
    
    child_populationdf["cost_TimeToKPITarget"] = chromosome_costs_time_to_target
    child_populationdf["cost_Priority"] = chromosome_costs_priority
    child_populationdf["cost_Distance"] = chromosome_costs_distance
      
    child_populationdf["cost_Total"] = child_populationdf["cost_TimeToKPITarget"] + child_populationdf["cost_Priority"] + child_populationdf["cost_Distance"]    
    populationdf = populationdf.append(child_populationdf)
    
#    populationdf = populationdf.nsmallest(population_size,columns="cost_Total")
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





















