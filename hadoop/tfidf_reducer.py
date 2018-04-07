#!/usr/bin/python2

def tfidf_reducer():
	import sys
	for line in sys.stdin:
		word,filename,tfidf = line.strip().split()
		print "%s\t%s\t%s" % (word,filename,tfidf)

if __name__ == '__main__':
	tfidf_reducer()