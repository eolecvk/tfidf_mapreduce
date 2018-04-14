from loremipsum import Generator
import threading

import time
import numpy as np
import os
import requests

def get_paragraphs(g, amount):
	return [x[-1] for x in g.generate_paragraphs(amount)]

def generate_docs(file_number, nb_paragraphs):

	filename = "/tmp/SherlockHolmes.txt"
	with open(filename, 'r') as sample_txt:
		s = sample_txt.read()#.decode('utf-8')
		s = s.split("\n")
		s = np.random.choice(s, len(s)//2)
		s = "\n".join(s)

	diconame = "/tmp/dictionary.txt"
	with open(diconame, 'r') as dico_txt:
		d = dico_txt.read()#.decode('utf-8')

	dictionary = d.replace("\n", " ").split()

	g = Generator(sample= s, dictionary=dictionary)
	outputfile = "/tmp/data/{}.txt".format(file_number)
	list_paragraphs = get_paragraphs(g, nb_paragraphs)

	with open(outputfile, "w") as f:
		f.write("\n\n".join(list_paragraphs))

def task(sc, nb_documents=5, nb_paragraphs=10):
	list_files = sc.parallelize(list(range(nb_documents)))
	gen_x_paragraphs = lambda x: generate_docs(x, nb_paragraphs=nb_paragraphs)
	list_files.map(gen_x_paragraphs).collect()

def download(url, fpath):
	r = requests.get(url)
	with open(fpath, "wb") as f:
		f.write(r.content)


if __name__ == "__main__":


	# DL source files
	url1 = 'https://raw.githubusercontent.com/dwyl/english-words/master/words.txt'
	url2 = 'http://www.gutenberg.org/cache/epub/1661/pg1661.txt'

	fpath1 = "/tmp/dictionary.txt"
	fpath2 = "/tmp/SherlockHolmes.txt"

	for url, fpath in [
		(url1, fpath1),
		(url2, fpath2) ]:
		download(url, fpath)

	# Get args
	import sys
	n_docs = int(sys.argv[1])
	n_paragraphs = int(sys.argv[2])

	print("n_docs", n_docs)
	print("n_paragraphs", n_paragraphs)

	# Set up pyspark path

	# Initialize spark session
	from pyspark import SparkContext, SparkConf
	# conf = SparkConf().setMaster('local[*]').setAppName('appname')
	# conf.set('spark.scheduler.mode', 'FAIR')
	# sc = SparkContext(conf=conf)
	# task(sc, nb_documents=n_docs, nb_paragraphs=n_paragraphs)

	sc = SparkContext.getOrCreate()
	#sc = SparkContext("local", "Simple App") (?)
	task(sc, nb_documents=n_docs, nb_paragraphs=n_paragraphs)

	#--Timit
	#start_time = time.time()
	#process...
	#end_time = time.time()
	#if not os.path.exists("outputs/"):
	#    os.makedirs("outputs/")
	#with open("outputs/results", "a") as f:
	#    f.write("Execution time : %s seconds." % (end_time - start_time))

