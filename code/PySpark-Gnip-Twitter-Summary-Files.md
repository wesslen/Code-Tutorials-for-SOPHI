## Step 1: Load Charlotte Sample Twitter Gnip Dataset

This step imports pySpark functions, reads in the Charlotte Gnip json file (Dec-Feb 2016 Geolocated Charlotte Tweets) and creates an RDD-file called tweets.

If you want to modify for a different dataset, change the path for the `jobDir` location. Also, modify the `outDir` to direct the file to where you want to save the files. More than likely, this will be your own personal folder in the `/user/` directory. 

Last, the `printSchema()` function will print the schema so you can see the structure of the dataset.

```{python}
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

#set input and output directories
jobDir = "/twitter/gnip/public/cities/Charlotte/charlotte062016.json"
outDir = "/user/rwesslen/Charlotte/"

tweets = sqlContext.read.format('json').load([jobDir])

#drops any blank records -- just in case
tweets = tweets.na.drop(subset=["id"])

tweets.count() # Count the number of items in the RDD

tweets.printSchema()  # Print the schema for tweets
```

## Step 2: Helper Functions

Now, let's call a collection of helper functions. These functions were used from Matteo Redaelli's [PySpark Tutorial](https://github.com/matteoredaelli/pyspark-examples).

```{python}
import json
import re
import sys
import time

import os,argparse

def javaTimestampToString(t):
  return time.strftime("%Y-%m-%d", t)

def cleanText(text):
  t = re.sub('["\']', ' ', unicode(text))
  return t.replace("\n"," ").replace("\t", " ").replace("\r", " ").replace("  ", " ")

def cleanTextForWordCount(text):
  # remove links
  t = re.sub(r'(http?://.+)', "", text)
  t = cleanText(t).lower()
  return re.sub('["(),-:!?#@/\'\\\]', ' ',t)

def count_items(rdd, min_occurs=2, min_length=3):
  return rdd.map(lambda t: (t, 1))\
            .reduceByKey(lambda x,y:x+y)\
            .filter(lambda x:x[1] >= min_occurs)\
            .filter(lambda x:x[0] is not None and len(x[0]) >= min_length)\
            .map(lambda x:(x[1],x[0])).sortByKey(False)\
.map(lambda x: '\t'.join(unicode(i) for i in x)).repartition(1)
```

## Step 3: Count files for time, user, device, language and user location

Let's create count files for a variety of variables including: hour, day, Twitter handle, device, language and user's profile location (open-ended text).

We'll then export out each of the count files to a tab-separated csv file within the out directory you selected in Step 1.

```{python}
from pyspark.sql.functions import col, substring

# Create Hour and Day Stamps
tweets = tweets.withColumn('HourStamp',substring("postedTime",1,13)).withColumn('DayStamp',substring("postedTime",1,10))

def toCSVLine(data):
  return '\t'.join(unicode(d) for d in data)

stats_hour = tweets.groupBy('HourStamp')/
    .count()/
    .orderBy('HourStamp')/
    .map(toCSVLine)
    
stats_hour.coalesce(1).saveAsTextFile(outDir + "HourCounts/")

stats_day = tweets.groupBy('DayStamp')/
    .count()/
    .orderBy('DayStamp')/
    .map(toCSVLine)
    
stats_day.coalesce(1).saveAsTextFile(outDir + "DayCounts/")

stats_users = tweets.groupBy('actor.preferredUsername')/
    .count()/
    .orderBy(col("count").desc())/
    .map(toCSVLine)
    
stats_users.coalesce(1).saveAsTextFile(outDir + "UserCounts/")

stats_device = tweets.groupBy('generator.displayName')/
    .count()/
    .orderBy(col("count").desc())/
    .map(toCSVLine)
    
stats_device.coalesce(1).saveAsTextFile(outDir + "DeviceCounts/")

stats_lang = tweets.groupBy('twitter_lang')/
    .count()/
    .orderBy(col("count").desc())/
    .map(toCSVLine)
    
stats_lang.coalesce(1).saveAsTextFile(outDir + "LangCounts/")

stats_location = tweets.groupBy('actor.location.displayName')/
    .count()/
    .orderBy(col("count").desc())/
    .map(toCSVLine)
    
stats_location.coalesce(1).saveAsTextFile(outDir + "LocationCounts/")
```

## Step 4: Run a word count.

This step will create a word count file.

```{python}
tweets.registerTempTable("tweets")
t = sqlContext.sql("SELECT distinct id, postedTime, body, twitter_entities.hashtags.text FROM tweets")

stats_words = count_items(t.map(lambda t: cleanTextForWordCount(t[2]))\
    .flatMap(lambda x: x.split()))
    
stats_words.coalesce(1).saveAsTextFile(outDir + "WordCounts/")
```

## Step 5: Run a hashtag count.

This step will create a similar hashtag count. Like the word count, the output file will not be sorted and you will need to sort the file.

```{python}
stats_hashtags = t.flatMap(lambda t: t[3])\
    .map(lambda t: (t.lower(), 1))\
    .reduceByKey(lambda x,y:x+y)\
    .map(lambda x:(x[1],x[0]))\
    .sortByKey(False)\
    .map(lambda x: '\t'.join(unicode(i) for i in x))\
    .repartition(1)

stats_hashtags.coalesce(1).saveAsTextFile(outDir + "/HashtagCounts/")
```
