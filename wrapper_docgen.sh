#!/bin/bash

for N_DOCS in $(seq 2 5 20)
do
	for N_PARAGRAPHS in $(seq 2 20 3000)
	do
		rm -rf /tmp/data
		mkdir /tmp/data

		echo "Generating input :\n\t${N_DOCS} documents\n\t${N_PARAGRAPHS} each"
		spark-submit "/home/eolus/Desktop/Dauphine/bigdata/tfidf/sourcing/generate_documents_pyspark.py" $N_DOCS $N_PARAGRAPHS

		#Spark
		LOG_SPARK=/tmp/log_spark_d$N_DOCS_p_$N_PARAGRAPHS.txt
		SPARK_TFIDF='/home/eolus/Desktop/Dauphine/bigdata/tfidf/spark/wrapper.sh';
		{ time ${SPARK_TFIDF} ; } 2> $LOG_SPARK;

		# Hadoop streaming
		LOG_HADOOP=/tmp/log_hadoop_d$N_DOCS_p_$N_PARAGRAPHS.txt
		HADOOP_TFIDF='/home/eolus/Desktop/Dauphine/bigdata/tfidf/hadoop/wrapper.sh';
		{ time ${HADOOP_TFIDF} ; } 2> $LOG_HADOOP;
	done
done
