import pdb
import pyodbc
import pandas as pd
from sklearn import preprocessing
import numpy as np
import random
import time
import os

import OptimiseTechPerformanceGA

OptimiseTechPerformanceGA.main(
    chromosome_capacity = 6 
    ,population_size_limit = 100
    ,convergence_generation = 1
    ,artificial_selection_fraction = 0.1 
    ,artificial_selection_sample_size = 0.25
    ,mutation_rate = 0.10 
    ,elite_size = 10
    ,hours_to_target_ignore_threshold = 100
    ,ResourceKey = 1620
    ,population_selection_type = "include artifical selection"
    ,crossover_type = "diagonal"
    ,parent_selection_type = "random"
    )

