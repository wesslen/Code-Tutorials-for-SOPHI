## Introduction

This code was built from [Databrick's LDA tutorial](https://docs.cloud.databricks.com/docs/latest/databricks_guide/index.html#05%20MLlib/2%20Algorithms/4%20LDA%20-%20Topic%20Modeling.html) with small modifications.

## Step 1: Load Twitter Gnip Dataset and create key-value pair

```{scala}
// SQL Context
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

val tweets = sqlContext.read.json("/twitter/gnip/public/social/blm.json").select("body")

val rdd = tweets.map{ row => (row.getString(0)) }.map(_.toLowerCase())

// Convert RDD to DF with ID for every document
val corpus_df = rdd.zipWithIndex.toDF("body", "id")
```

## Step 2: Tokenize the tweets

```{scala}
import org.apache.spark.ml.feature.RegexTokenizer

// Set params for RegexTokenizer
val tokenizer = new RegexTokenizer()
    .setMinTokenLength(4)
    .setInputCol("body")
    .setOutputCol("tokens")
    .setPattern("[\\W_]+")

// Tokenize document
val tokenized_df = tokenizer.transform(corpus_df)
```

## Step 3: Remove stop words. 

For this step, you will need to save the `stop_words` text file locally. You can get the file [here](http://ir.dcs.gla.ac.uk/resources/linguistic_utils/stop_words). Make sure to update the chunk for the directory where you placed the file.

```{scala}
import org.apache.spark.ml.feature.StopWordsRemover

// List of stopwords
val stopwords = sc.textFile("/user/rwesslen/exLDA/stop_words").collect()

// New stop words
val add_stopwords = Array("https","amp","http","rt")

// Combine newly identified stopwords to our exising list of stopwords
val new_stopwords = stopwords.union(add_stopwords)

// Set params for StopWordsRemover
val remover = new StopWordsRemover().setStopWords(new_stopwords).setInputCol("tokens").setOutputCol("filtered")

// Create new DF with Stopwords removed
val filtered_df = remover.transform(tokenized_df)
```

## Step 4: Create the count vectors

```{scala}
import org.apache.spark.ml.feature.CountVectorizer

// Set params for CountVectorizer
val vectorizer = new CountVectorizer().setInputCol("filtered").setOutputCol("features").setVocabSize(10000).setMinDF(5).fit(filtered_df)

// Create vector of token counts
val countVectors = vectorizer.transform(filtered_df).select("id", "features")

// Convert DF to RDD
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql._

val lda_countVector = countVectors.map { case Row(id: Long, countVector: Vector) => (id, countVector) }
```

## Step 5: Running LDA with Online Variational Bayes

```{scala}
val numTopics = 20

import org.apache.spark.mllib.clustering.{LDA, OnlineLDAOptimizer}

// Set LDA params
val lda = new LDA().setOptimizer(new OnlineLDAOptimizer()
    .setMiniBatchFraction(0.8))
    .setK(numTopics)
    .setMaxIterations(10)
    .setDocConcentration(-1) // use default values
    .setTopicConcentration(-1) // use default values

val ldaModel = lda.run(lda_countVector)

// Review Results of LDA model with Online Variational Bayes
val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
val vocabList = vectorizer.vocabulary
val topics = topicIndices.map { case (terms, termWeights) =>
  terms.map(vocabList(_)).zip(termWeights)
}
println(s"$numTopics topics:")
topics.zipWithIndex.foreach { case (topic, i) =>
  println(s"TOPIC $i")
  topic.foreach { case (term, weight) => println(s"$term\t$weight") }
  println(s"==========")
}
```

## Step 6: Alternatively, running LDA with Expectation-Maximization

```{scala}
val numTopics = 20

import org.apache.spark.mllib.clustering.{LDA, OnlineLDAOptimizer}

// Set LDA parameters
val em_lda = new LDA()
    .setOptimizer("em")
    .setK(numTopics)
    .setMaxIterations(100)
    .setDocConcentration(-1) // use default values
    .setTopicConcentration(-1) // use default values

val em_ldaModel = em_lda.run(lda_countVector)

val topicIndices = em_ldaModel.describeTopics(maxTermsPerTopic = 10)
val vocabList = vectorizer.vocabulary
val topics = topicIndices.map { case (terms, termWeights) =>
  terms.map(vocabList(_)).zip(termWeights)
}
println(s"$numTopics topics:")
topics.zipWithIndex.foreach { case (topic, i) =>
  println(s"TOPIC $i")
  topic.foreach { case (term, weight) => println(s"$term\t$weight") }
  println(s"==========")
}
```

## Step 7: Export out top words for EM LDA Topics

```{scala}
// Zip topic terms with topic IDs
val termArray = topics.zipWithIndex

// Transform data into the form (term, probability, topicId)
val termRDD = sc.parallelize(termArray)
val termRDD2 =termRDD.flatMap( (x: (Array[(String, Double)], Int)) => {
  val arrayOfTuple = x._1
  val topicId = x._2
  arrayOfTuple.map(el => (el._1, el._2, topicId))
})

// Create DF with proper column names
val termDF = termRDD2.toDF.withColumnRenamed("_1", "term").withColumnRenamed("_2", "probability").withColumnRenamed("_3", "topicId")

// Create JSON data
val rawJson = termDF.toJSON.collect().mkString(",\n")

termDF.rdd.coalesce(1).saveAsTextFile("/twitter/gnip/secure/rwesslen/EM-LDA-Results/")
```

## Step 8: How to save your results (with the code on how to load for next time)

```{scala}
em_ldaModel.save(sc, "/twitter/gnip/secure/rwesslen/EM-LDA-Raw-Results/")

val sameModel = DistributedLDAModel.load(sc,"/twitter/gnip/secure/rwesslen/EM-LDA-Raw-Results/")
```

