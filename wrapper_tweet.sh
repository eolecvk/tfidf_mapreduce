#!/bin/bash

# Data input
N_TWEETS=100

for N_TWEETS in $(seq 1000 1000 14000)
do
	# Generate doc by some method, save size as variable
	rm -rf /tmp/data
	mkdir /tmp/data

	echo "Generating input (${N_TWEETS} tweets)"
	/usr/bin/python3 "/home/eolus/Desktop/Dauphine/bigdata/tfidf/sourcing/sourcing_wrapper.py" $N_TWEETS

	# TFIDF with timer

	# Spark
	LOG_SPARK=/tmp/log_spark_$N_TWEETS.txt
	SPARK_TFIDF='/home/eolus/Desktop/Dauphine/bigdata/tfidf/spark/wrapper.sh';
	{ time ${SPARK_TFIDF} ; } 2> $LOG_SPARK;

	# Hadoop streaming
	LOG_HADOOP=/tmp/log_hadoop_$N_TWEETS.txt
	HADOOP_TFIDF='/home/eolus/Desktop/Dauphine/bigdata/tfidf/hadoop/wrapper.sh';
	{ time ${HADOOP_TFIDF} ; } 2> $LOG_HADOOP;
done

