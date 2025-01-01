#%%
import pandas as pd
from run_process import run_selection
import os

DESIRED_INDUSTRY = 'services'
DESIRED_SUBINDUSTRY = 'travel_hospitality_services'
DESIRED_USECASE = 'traditional_ads'
DESIRED_SUBUSECASE = 'out_of_home_ads'
DESIRED_PLATFORM = 'all'
DESIRED_DEVICE = 'all'

run_selection(DESIRED_INDUSTRY, DESIRED_SUBINDUSTRY, DESIRED_USECASE, DESIRED_SUBUSECASE, DESIRED_PLATFORM, DESIRED_DEVICE)

# %%
