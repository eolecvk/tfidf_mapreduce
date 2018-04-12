

def load_docs(dpath):
	import os
	docs = []
	for fname in os.listdir(dpath):
		fpath = "{}/{}".format(dpath, fname)
		try:
			with open(fpath, 'r') as fp:
				doc = fp.read()
				doc_id = int(fname.replace(".txt", ""))
				docs.append( (doc_id, doc) )
		except Exception as e:
			print(e)
	return docs


if __name__ == "__main__":

	# Load input docs
	dpath_docs = "/tmp/data"
	docs = load_docs(dpath_docs)

	# Initialize spark session
	from pyspark.sql import SparkSession, SQLContext
	sc = SparkSession\
			.builder\
			.appName("TfIdf Example")\
			.getOrCreate()
	sql = SQLContext(sc)

	# Load input as spark df
	doc_df = sql.createDataFrame(docs, [ "doc_id", "doc_text" ])

	# Tokenize transform
	from pyspark.ml.feature import RegexTokenizer
	tokenizer = RegexTokenizer(inputCol="doc_text", outputCol="words", pattern="\\W")
	token_df = tokenizer.transform(doc_df)

    # TF transform
	from pyspark.ml.feature import HashingTF
	htf= HashingTF(inputCol="words", outputCol="tf")
	tf_df = htf.transform(token_df)

	# IDF transform
	from pyspark.ml.feature import IDF
	idf = IDF(inputCol="tf",outputCol="idf")
	tfidf_df = idf.fit(tf_df).transform(tf_df)

	# Save output
	import datetime
	ts_suffix = str(datetime.datetime.now())
	dst_fpath = "/tmp/spark_output_{}.pkl".format(ts_suffix)

	import pandas
	tfidf_df.toPandas().to_pickle(dst_fpath)
	print("Spark TFIDF output successfully saved to: {}".format(dst_fpath))