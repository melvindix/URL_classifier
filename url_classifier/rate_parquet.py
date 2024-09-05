import pandas as pd
import numpy as np
from tld import get_fld
import csv

countries = ['de']#,'en','it','nl','ru','sveden','bulgaria','estonia','hungary','indonesia','israel','poland','turkey']
strings_to_filter = ['t.me', 'youtube.com', 'paypal.com', 'facebook.com','youtu.be','zoom.us',
                     'docs.google.com','twitter.com', 'gofundme.com', 'drive.google.com','bit.ly']


rating_path = '/data.nst/mdix/telegram/domain-pc1.csv'
data_dict = {}
with open(rating_path, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        # Skip the header row
        next(reader, None)
        for row in reader:
            # Assuming there are only two columns in each row
            if len(row) == 2:
                key, value = row
                data_dict[key] = float(value)


def filter_and_print_percentage(dataframe, filter_strings):
    # Assuming the column 'url' exists in the dataframe
    if 'url' not in dataframe.columns:
        raise ValueError("Column 'url' not found in the dataframe.")

    # Validate filter_strings
    if not isinstance(filter_strings, list) or not all(isinstance(s, str) for s in filter_strings):
        raise ValueError("filter_strings should be a list of strings.")

    # Calculate the percentage of rows that will be filtered
    total_rows = len(dataframe)
    filtered_rows = len(dataframe[dataframe['url'].str.contains('|'.join(filter_strings))])
    percentage_filtered = (filtered_rows / total_rows) * 100

    # Print the percentage
    print(f"Percentage of rows containing specified strings: {percentage_filtered:.2f}%")

    # Filter rows based on the condition
    filtered_dataframe = dataframe[~dataframe['url'].str.contains('|'.join(filter_strings))]

    return filtered_dataframe





for country in countries:
    print(country)

    #'''
    data = pd.read_parquet('/data.nst/mdix/telegram/data/urls/{}.parquet'.format(country), engine='pyarrow')

    #save how many urls were posted
    date_counts = data.groupby(data['time'].dt.date).size()
    dates_array = np.array(date_counts.index)
    result_array = np.vstack((dates_array, date_counts.values))
    np.save('/data.nst/mdix/telegram/data/urls/total_url_frequency/{}_total_frequency.npy'.format(country), result_array)
    
    filtered_data = filter_and_print_percentage(data, strings_to_filter)

    n = filtered_data.shape[0]
    classified_counter=0
    ratings = []

    vals = filtered_data.values
    for i in range(n):
        url = vals[i,0]
        domain = get_fld(url, fail_silently=True)
        if domain in data_dict:
            
            #print(url, data_dict[domain])
            ratings.append([vals[i,1].to_pydatetime(), data_dict[domain]])
            classified_counter+=1

    print('Classified URLs:{:.2f}%'.format(classified_counter/n*100))

    ratings = np.array(ratings)
    np.save('/data.nst/mdix/telegram/data/urls/ratings/{}_ratings.npy'.format(country), ratings)
    #'''
    ratings = np.load('/data.nst/mdix/telegram/data/urls/ratings/{}_ratings.npy'.format(country), allow_pickle=True)
    # Convert it to a pandas DataFrame for easier manipulation
    ratings_df = pd.DataFrame(ratings, columns=['datetime', 'r'])

    # Convert 'datetime' column to datetime type
    ratings_df['datetime'] = pd.to_datetime(ratings_df['datetime'])
    # Set 'datetime' as the index
    ratings_df.set_index('datetime', inplace=True)

    # Resample the data to daily frequency and calculate the mean
    mean_ratings = ratings_df.resample('D')['r'].mean()
    freq = ratings_df.resample('D')['r']
    freq = freq.size().values

    save_data = np.vstack([np.array(mean_ratings.index), np.array(mean_ratings.values),np.array(freq)])
    np.save('/data.nst/mdix/telegram/data/urls/ratings/{}_timeseries_ratings.npy'.format(country),save_data)




