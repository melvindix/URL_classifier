"""
Scans a collection of messages for all occuring urls and matches messages that contain the same urls
"""

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

import pandas as pd
import re
import pickle


####
# Connect to database
####

print('START')

load_dotenv()

from sqlalchemy import URL
from sqlalchemy import create_engine

url_object = URL.create(
    "mariadb+mariadbconnector",
    username=os.getenv("DB_USER"),
    password=os.getenv("DB_USER_PASSWD"),
    host="127.0.0.1",
    port=7256,
    database="telegram_data",
)
engine = create_engine(url_object)

print("CONNECTED")
####
# Scan messages
#####

max_id = 2150000000
id_stepsize = 10000000


for i in range(18, max_id//id_stepsize + 1):
    id_start = i*id_stepsize
    print('searching from ',id_start, ' to ', id_start+id_stepsize)
    #msg = pd.read_sql_query("SELECT chat_id,date,text FROM messages WHERE text LIKE '%breitbart.com%' AND chat_id < 10",engine) # date BETWEEN '2023-06-01' AND '2023-06-02';",engine)
    msg = pd.read_sql_query(f"SELECT message_id,url FROM entities WHERE url IS NOT NULL AND id BETWEEN {id_start} AND {id_start+id_stepsize}" ,engine) # date BETWEEN '2023-06-01' AND '2023-06-02';",engine)
    print("SEARCHING DONE")

    ####cd
    # Save results
    ####

    msg.to_parquet(f'/data.nst/mdix/telegram/data/urls/breitbart/breitbart_{i}.parquet')
    #id_start += id_stepsize

print("DONE")