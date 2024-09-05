import pandas as pd
import pickle
import time

countries = ['bulgaria','estonia','hungary','indonesia','israel','poland','turkey']

class URLInfo:
    def __init__(self,url,chat,time,msg_id):
        self.url = url
        self.msg = [(chat,time,msg_id)]
        self.unique_chats = {chat}
    
    def add(self,chat,time,msg_id):
        self.msg.append((chat,time,msg_id))
        self.unique_chats.add(chat)

for country in countries:
    print(country)
    start=time.time()
    filepath = '/data.nst/rventzke/telegram-project/urls/{}.all.pickle'.format(country)
    data = pd.read_pickle(filepath)

    url_list = []
    time_list = []
    chat_list = []
    msg_id_list = []

    repost_list = []
    print(len(data))

    i=0
    repost_count = 0
    for key in data:
        reposts = len(data[key].msg)
        repost_list.append(reposts)
        repost_count+=reposts
        for j in range(reposts):
            #print(i, len(data[key].msg))
            #print(data[key].url)
            url_list.append(data[key].url)
            time_list.append(data[key].msg[j][1])
            chat_list.append(data[key].msg[j][0])
            msg_id_list.append(data[key].msg[j][2])
    print('r:',repost_count)

    pandas_data = {'url': url_list,
            'time': time_list,
            'chat_id': chat_list,
            'msg_id': msg_id_list}

    # Create DataFrame
    df = pd.DataFrame(pandas_data)

    df.to_parquet('/data.nst/mdix/telegram/data/urls/{}.parquet'.format(country))

    end = time.time()
    print('time:',(end-start))