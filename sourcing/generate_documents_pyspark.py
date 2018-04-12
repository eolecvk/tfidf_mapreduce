from loremipsum import Generator
import threading
from pyspark import SparkContext, SparkConf

def get_paragraphs(g, amount):
	return [x[-1] for x in g.generate_paragraphs(amount)]

def generate_docs(file_number):
	filename = "/texts/SherlockHolmes.txt"
	with open(filename, 'r') as sample_txt:
		s = sample_txt.read()

	diconame = "/texts/dictionary.txt"
	with open(diconame, 'r') as dico_txt:
		d = dico_txt.read()

	dictionary = d.replace("\n", " ").split()

	g = Generator(sample= s, dictionary=dictionary)
	list_paragraphs = get_paragraphs(g, 1000)

	outputfile = "/outputs/" + str(file_number) + ".txt"

	for i in range(10):
		if i == 0:
			with open(outputfile, "w") as f:
				f.write("\n".join(list_paragraphs))
		else:
			with open(outputfile, "a") as f:
			f.write("\n".join(list_paragraphs))

def task(sc):
	list_files = sc.parallelize(list(range(10)))
	list_files.map(generate_docs).collect()


if __name__ == "__main__":
	
	conf = SparkConf().setMaster('local[*]').setAppName('appname')
	conf.set('spark.scheduler.mode', 'FAIR')
	sc = SparkContext(conf=conf)
	task(sc)

