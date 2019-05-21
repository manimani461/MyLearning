# Databricks notebook source
subscription_key = '5dd3892e25454d9bb464eceb63d7aa6a'
assert subscription_key

# COMMAND ----------

import requests
from pprint import pprint
text_analytics_base_url = "https://westeurope.api.cognitive.microsoft.com/text/analytics/v2.0/"
sentiment_api_url = text_analytics_base_url + "sentiment"
print(sentiment_api_url)
documents = 'dbfs:/mnt/kp-adls/TwitterData.txt'
headers   = {"Ocp-Apim-Subscription-Key": subscription_key}
response  = requests.post(sentiment_api_url, headers=headers, json=documents)
sentiments = response.json()
pprint(sentiments)

# COMMAND ----------

#data = sc.textFile("dbfs:/mnt/kp-adls/advertising.csv")
data = spark.read.option("multiline", "true").json("dbfs:/mnt/kp-adls/TwitterData.txt")

# COMMAND ----------

data.printSchema()

# COMMAND ----------

data.dtypes

# COMMAND ----------

data = data.select("created_at","id","text","source",(data.user.id).alias("UserID"),data.user.location,data.user.description,data.user.followers_count,data.user.friends_count,data.user.created_at,data.user.time_zone)
data.view(truncate=True)
#data.repartition(1).write.json("dbfs:/mnt/kp-adls/TwitterData2.json")

# COMMAND ----------

lines=[line.strip() for line in data]
text=sc.textFile(','.join(lines)).cache()
print_count(text)

# COMMAND ----------


