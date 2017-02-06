## Step 1: Load Charlotte Sample Twitter Gnip Dataset

This step imports pySpark functions, reads in the Charlotte Gnip json file (Dec-Feb 2016 Geolocated Charlotte Tweets) and creates an RDD-file called tweets.

Last, the `printSchema()` function will print the schema so you can see the structure of the dataset.

```{python}
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

# Create an RDD called tweets
tweets = sqlContext.read.json("/twitter/gnip/public/cities/Charlotte/charlotte062016.json")

# Count the number of items in the RDD
tweets.count() 

# Print the schema for tweets
tweets.printSchema()
```

## Step 2: Basic GroupBy & Count Functions

Next, let's explore two fields to groupby and count the tweets.

First, we groupBy the `verb` field which corresponds to Tweets (post) and Retweets (share). Notice, this dataset only includes posts as the filtering rules for this dataset excluded Retweets.

Next, we groupBy the `geo.type` field to identify whether the geolocated Tweets are points or places (NULL).

```{python}
#Retweets (share) vs Original Content Posts (post)
tweets.groupBy("verb").count().show()

#Geolocated Points vs non-Geolocated Points
tweets.groupBy("geo.type").count().show()
```

## Step 3: groupBy, filter and orderBy functions

We can also include the filter and orderBy functions too to limit or sort our data.

```{python}
from pyspark.sql.functions import col

# by top 20 locations
tweets.groupBy("actor.location.displayName")/
    .count()/
    .orderBy(col("count").desc())/
    .show()

# Tweets by users with more than 500k Followers Ordered by Tweet Time (postedTime)
tweets.filter(tweets['actor.followersCount'] > 500000)/
    .select("actor.preferredUsername","body","postedTime","actor.followersCount")/
    .orderBy("postedTime")/
    .show()
```

## Step 4: If-then Statements Using Case

You can import in the `when` function that is used like a case when (if-else) statement.

```{python}
from pyspark.sql.function import when

tweets = tweets.withColumn("geotype", (when(col("verb") == "share", "Retweet").otherwise("Post")))
```

## Step 5: Registering the Dataframe as a Table

Alternatively to creating RDD's, you can create a Spark DataFrame by registering the dataframe as a table.

```{python}
tweets.registerTempTable('tweets')

df = sqlContext.sql("SELECT id, postedTime, body, actor.id FROM tweets")
```

## Step 6: Export to CSV

This step will export your file to a CSV. There are other alternatives to exporting csvs (namely [spark-csv](https://github.com/databricks/spark-csv)) but these are not yet available on SOPHI. We'll update the code with instructions when it is available.

```{python}
import csv
import cStringIO

def row2csv(row):
    buffer = cStringIO.StringIO()
    writer = csv.writer(buffer)
    writer.writerow([str(s).encode("utf-8") for s in row])
    buffer.seek(0)
    return buffer.read().strip()

df.rdd.map(row2csv).coalesce(1).saveAsTextFile("file.csv")
```
