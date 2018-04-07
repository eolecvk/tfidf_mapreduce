#!/usr/bin/python2

def tfidfmapper():
	import os
	import sys
	for line in sys.stdin:
		word,filename,wordcount,count = line.strip().split()
		tfidf = eval(wordcount) / (eval(count) * 1.0)
		print "%s\t%s\t%s" % (word,filename, tfidf)


if __name__ == '__main__':
	tfidfmapper()

