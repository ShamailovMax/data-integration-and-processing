import os
import numpy as np
import pandas as pd
import psycopg2

from driveToPostgres.config import *


df = pd.read_csv('C:\\data\\combined_data.csv')
df.head()

# clean data table name
file = 'combined Data'
clean_data_table = file.lower().replace(" ", "") \
                               .replace("?", "") \
                               .replace("-", "_") \
                               .replace(r"/", "_") \
                               .replace("\\", "_") \
                               .replace("%", "") \
                               .replace(")", "") \
                               .replace(r"(", "") \
                               .replace("$", "")

# clean headers name
df.columns = [
    x.lower().replace(" ", "") \
             .replace("?", "") \
             .replace("-", "_") \
             .replace(r"/", "_") \
             .replace("\\", "_") \
             .replace("%", "") \
             .replace(")", "") \
             .replace(r"(", "") \
             .replace("$", "")
    for x in df.columns
]