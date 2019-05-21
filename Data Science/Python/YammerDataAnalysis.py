# -*- coding: utf-8 -*-
"""
Created on Fri Jan 25 19:15:13 2019

@author: mabotula
"""

import pandas as pd
import numpy as np
import http.client, urllib
import json
import urllib.request
import urllib.response

messages = pd.read_csv("C:/Users/mabotula/Documents/data/yammer/export-1548402054719/Messages.csv",doublequote = True)
messages.columns
messages['replied_to_id'] = messages['replied_to_id'].fillna(0)
messages['replied_to_id'] = messages['replied_to_id'].astype(int)
messages['replied_to_id'].head(5)

messages_groupbypost = messages.groupby(['thread_id']).size().reset_index(name='count')
messages_TopPosts = messages_groupbypost.query('count > 35')

result = pd.merge(messages, messages_TopPosts, how='inner', on=['thread_id', 'thread_id'])
result.columns
result.count()


subscription_key = 'ca26bbb92a564fd8bc96febba880e499'
assert subscription_key

text_analytics_base_url = "https://westeurope.api.cognitive.microsoft.com/text/analytics/v2.0/"
sentiment_api_url = text_analytics_base_url + "sentiment"
print(sentiment_api_url)

def parseDF(df):
    documents = { 'documents': []}
    #count = 1
    for index, row in df.iterrows():
        text = row['body']
        dfid = row['id']
        documents.setdefault('documents').append({"language":"en","id":str(dfid),"text":text})
        #count+= 1
    return documents
df_parse= result[['id','body']]
docs = parseDF(df_parse)

accessKey = 'ca26bbb92a564fd8bc96febba880e499'
url = 'westeurope.api.cognitive.microsoft.com'
#path = '/text/analytics/v2.0/Sentiment'
path = '/text/analytics/v2.0/keyPhrases'
def TextAnalytics(documents,path):
    headers = {'Ocp-Apim-Subscription-Key': accessKey}
    conn = http.client.HTTPSConnection(url)
    body = json.dumps (documents)
    path = path
    conn.request ("POST", path, body, headers)
    response = conn.getresponse ()
    return response.read ()

data_sentiment = TextAnalytics (docs,path)
jdata = json.loads(data_sentiment)
df_sentiment = pd.io.json.json_normalize(jdata['documents'])
df_sentiment.columns
df_sentiment['id'] = df_sentiment['id'].astype(int)
df_withsentiment = pd.merge(result, df_sentiment, how='inner', on=['id', 'id'])
df_withsentiment.columns

data_keyphrase = TextAnalytics (docs,path)
jdata = json.loads(data_keyphrase)
df_keyphrase = pd.io.json.json_normalize(jdata['documents'])
df_keyphrase.columns
df_keyphrase['id'] = df_keyphrase['id'].astype(int)
df_withkeyphrase = pd.merge(df_withsentiment, df_keyphrase, how='inner', on=['id', 'id'])
df_withkeyphrase.columns
df_withkeyphrase.count()
#df_withkeyphrase.to_csv(path_or_buf = "C:/Users/mabotula/Documents/data/yammer/export-1548402054719/messages_Processed.csv",quotechar = '"',index=False)
messages_Post = df_withkeyphrase.query('replied_to_id == 0')
messages_Post.to_csv(path_or_buf = "C:/Users/mabotula/Documents/data/yammer/export-1548402054719/messages_Post.csv",quotechar = '"',index=False)
messages_Post.count()
messages_Comment = df_withkeyphrase.query('replied_to_id != 0')
messages_Comment.to_csv(path_or_buf = "C:/Users/mabotula/Documents/data/yammer/export-1548402054719/messages_Comment.csv",quotechar = '"',index=False)
messages_Comment.count()
