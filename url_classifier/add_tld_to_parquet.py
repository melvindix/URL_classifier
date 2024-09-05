import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from tld import get_fld

countries = ['en','de']

for country in countries:
    print(country)
    print('reading data...')
    data = pd.read_parquet('/data.nst/mdix/telegram/data/urls/{}.parquet'.format(country), engine='pyarrow')
    print('reading data finished')

    #add top-level domain to the dataframe
    data['tld'] = ''
    for index, url in enumerate(data['url']):
        data.at[index,'tld'] = get_fld(url, fail_silently=True)

    data.to_parquet('/data.nst/mdix/telegram/data/urls/{}_tld.parquet'.format(country))

