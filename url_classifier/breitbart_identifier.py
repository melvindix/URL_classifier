"""
Searches for breitbart.com URLs and adds chat_id and chat_name to the URLs
"""



import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

import pandas as pd
import re
import pickle
import re2
from tld import get_fld

print('START')

####
# Connect to database
####

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

# Function to fetch chat_id from the database for a given message_id
def fetch_chat_id(message_id):
    query = pd.read_sql_query(f"SELECT chat_id FROM messages WHERE id = {message_id}" ,engine)
    return query['chat_id']
# Function to fetch chat name from chat_id
def fetch_chat_name(chat_id):
    query = pd.read_sql_query(f"SELECT name FROM chats WHERE id = {chat_id}" ,engine)
    return query['name']


#First go through all URLs, check if they have breitbart in them and then store them together

# Function to check if a URL belongs to breitbart.com
def is_breitbart(url):
    try:
        domain = get_fld(url)
        return re2.match(r'^breitbart\.com$', domain) is not None
    except:
        return False

# List to store all unique breitbart DataFrames
#all_breitbart_dfs = []

print('check for breitbart URLs')

for i in range(81,215):
    print(i)
    # Load the DataFrame from a Parquet file
    df = pd.read_parquet(f'/data.nst/mdix/telegram/data/urls/breitbart/breitbart_{i}.parquet')
    # Apply the function to filter URLs from breitbart.com
    df['is_breitbart'] = df['url'].apply(is_breitbart)
    # Filter the DataFrame for rows matching breitbart.com
    breitbart_df = df[df['is_breitbart']]
    # Drop duplicates based on 'message_id'
    breitbart_unique_df = breitbart_df.drop_duplicates(subset='message_id')
    # Optionally, drop the 'is_breitbart' column
    final_breitbart_df = breitbart_unique_df.drop(columns=['is_breitbart'])

    final_breitbart_df['chat_id'] = final_breitbart_df['message_id'].apply(fetch_chat_id)
    final_breitbart_df['chat_name'] = final_breitbart_df['chat_id'].apply(fetch_chat_name)
    final_breitbart_df.to_parquet(f'/data.nst/mdix/telegram/data/urls/breitbart/breitbart_chats_{i}.parquet')

    # Append the unique DataFrame to the list
    #all_breitbart_dfs.append(breitbart_unique_df)

# Concatenate all the unique DataFrames into one
#final_breitbart_df = pd.concat(all_breitbart_dfs, ignore_index=True)
# Drop duplicates again, in case there were duplicates across different files
#final_breitbart_df = final_breitbart_df.drop_duplicates(subset='message_id')

#save all breitbart URLs
#final_breitbart_df.to_parquet('/data.nst/mdix/telegram/data/urls/breitbart/urls_with_breitbart.parquet')

#now check for all messages the group where it comes from


# Apply the fetch_chat_id function to each message_id in the DataFrame

print("DONE")