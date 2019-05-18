import datetime
import pytz
import time
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import split
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import sys,tweepy,csv,re
from textblob import TextBlob
import matplotlib.pyplot as plt
import matplotlib.patches
import pandas as pd

datetime_obj = datetime.datetime.strptime('Sun Oct 12 10:53:51 +0000 2014', '%a %b %d %H:%M:%S +0000 %Y')
print (type(datetime_obj), datetime_obj.isoformat())
ts = time.strftime('%Y-%m-%d', time.strptime('Sun Oct 12 10:53:51 +0000 2014','%a %b %d %H:%M:%S +0000 %Y'))
print(type(ts))

spark = SparkSession\
.builder\
.appName("HashtagCount")\
.getOrCreate()
df = spark.read.json("hashtags2.txt")
df.createOrReplaceTempView("hashtags")


sqldf = spark.sql(" SELECT sum(case when upper(text) like '%RCB%' or text like '%rcb%' then 1 else 0 end) as RCB,\
sum(case when upper(text) like '%CSK%' or text like '%csk%' then 1 else 0 end) as CSK,\
sum(case when upper(text) like '%MI%' or text like '%mi%' then 1 else 0 end) as MI,\
sum(case when upper(text) like '%SRH%' or text like '%srh%' then 1 else 0 end) as SRH,\
sum(case when upper(text) like '%KingsXIPunjab%' or text like '%KingsXIPunjab%' then 1 else 0 end) as punjab,\
sum(case when upper(text) like '%DelhiCapitals%' or text like '%DelhiCapitals%' then 1 else 0 end) as DelhiCapitals,\
sum(case when upper(text) like '%RR%' or text like '%rr%' then 1 else 0 end) as RajasthanRoyals,\
sum(case when upper(text) like '%KKR%' or text like '%kkr%' then 1 else 0 end) as KKR FROM hashtags ")
sqldf.show(150)
#sqldf.toPandas().to_csv('topUsers.csv')