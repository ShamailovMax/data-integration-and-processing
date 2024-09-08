import os
import numpy as np
import pandas as pd
import psycopg2

from config import *


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

# print(df.columns)


# mapping 
replacements = {
    'float64'            : 'decimal',
    'object'             : 'varchar',
    'int64'              : 'int',
    'datetime64'         : 'timestamp',
    'timedelta64[ns]'    : 'varchar'
}

col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(df.columns, df.dtypes.replace(replacements)))
# print(col_str)

# data connection to db
conn_string = f"host={host} \
                dbname='{db_name}' \
                user='{user}' \
                password='{password}'"

# try:
#     conn = psycopg2.connect(conn_string)
#     print("Connection successful!")
# except:
#     print("Connection unsuccessful.")

conn = psycopg2.connect(conn_string)
print('db connected')

cursor = conn.cursor()

# drop table with same name
cursor.execute(f"drop table if exists {clean_data_table}")

# create table
cursor.execute(f"create table {clean_data_table} \
                ({col_str})")

# save df to csv
df.to_csv(f'{clean_data_table}.csv', header=df.columns, index=False)


my_file = open(f'{clean_data_table}.csv')
print('file opened in memory')

# upload ready csv-file to db
SQL_STATEMENT = f"""
COPY {clean_data_table} FROM STDIN WITH
    CSV
    HEADER
    DELIMITER AS ','
"""

cursor.copy_expert(sql=SQL_STATEMENT, file=my_file)
print('file copied to db')

cursor.execute(f'grant select on table {clean_data_table} to public')
conn.commit()

cursor.close()
print(f'table {clean_data_table} imported to db completed')
