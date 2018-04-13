--------------------
Note:
Problem with spark implementation:  
The instruction is to come up with a single mapreduce algo and then implement it using both framework....
Need to reproduce TFIDF hadoop flow in spark.
--------------------



# TFIDF

**_Implementations of TFIDF mapreduce algorithm using Spark and Hadoop streaming._**

_Consider the problem of calculating the TF-IDF score for each word in a set of documents for each document. Provide a MapReduce algorithm to calculate TF-IDF scores given an input set of documents. Provide both a MapReduce Python Hadoop streaming and a Spark implementation. Perform experimental analysis in order to compare performances of the two implementation. To test scalability consider 5 document sets of increasing sizes. For instance, size can double from a set to another._

_The set of input textual document can be either dowloaded from the Web or generated by a Python script for instance. To speed up document generation MapReduce can be used, for instance by requiring that n Reducers generate a certain amount of documents each. Words of documents can be randomly picked from an input fixed vocabulary. Also, you can use Spark to make document generation easier._



## Pipeline

1. Sourcing
2. Preprocessing
3. TFIDF (spark and hadoop)
4. Bash timer


### Sourcing

We will be using tweets text as input documents for this experiment. The tweets are JSON objects with a non deterministic structure.

We will only use the `text` field value as documents for this experiment. The `text` field consists of a string of 140 characters or less which is displayed on the platform when a user is _'tweeting'_.

The tweets can be fetched through the [Twitter streaming API](https://developer.twitter.com/en/docs/tweets/filter-realtime/overview).

We used [this project](https://github.com/eolecvk/twitter_toolkit) to capture a stream of tweets up until the point we had 14,000 tweets to work with.


### Preprocessing

We used a [python script](https://github.com/eolecvk/tfidf_mapreduce/blob/master/sourcing/sourcing_wrapper.py) to extract the text value from the tweet JSON object and save it as a text document to be used by the tfidf mapreduce programs.

A sample of 100 tweet documents is available [here](https://github.com/eolecvk/tfidf_mapreduce/tree/master/data).


### TFIDF

#### Hadoop

We are using the hadoop streaming API.
It makes it easy to create mapper and reducer as python scripts using `sys.stdin` and `sys.stdout` for I/O.

The hadoop pipeline for TFIDF consists in 3 successive mapreduce tasks:

1. to get term-frequency for each term-document pairs
2. to get document-frequency for each term
3. to get tfidf score for each term-document pair

#### Spark

We are using the spark python API which offers optimized functions for each steps of the TFIDF program.


### Timer wrapper

We run the hadoop and the spark programs using an increasingly large corpus of tweets (from 1000 to 14000 with a step of 1000) and measure the time performance using the `time` builtin bash function.

```
time my_script.sh

real 	3m07,982
sys 	0m27,202
real 	0m09,232
```




## Results

Environment:

+ Single-node cluster
+ Hardware:
	+ XPS13-9360
	+ processor: i7 7th gen
	+ memory: 16GB
+ OS: Ubuntu 17.04
+ Spark: spark-2.1.1-bin-hadoop2.7
+ Hadoop: hadoop2.7

The full log data is archived [here](https://github.com/eolecvk/tfidf_mapreduce/blob/master/log/log_txt.zip).

### Hadoop scalability results

![Scalability performance: hadoop implementation](https://github.com/eolecvk/tfidf_mapreduce/blob/master/log/hadoop_chart.png)

![Log data](https://github.com/eolecvk/tfidf_mapreduce/blob/master/log/hadoop_log.png)

**Comment:**
...

### Spark scalability results

![Scalability performance: spark implementation](https://github.com/eolecvk/tfidf_mapreduce/blob/master/log/spark_chart.png)

![Log data](https://github.com/eolecvk/tfidf_mapreduce/blob/master/log/spark_log.png)

**Comment:**
...