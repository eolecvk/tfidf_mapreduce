#!/usr/bin/python2

def extract_words(input_line):
	import re
	words = re.findall(r'\w+', input_line) # extract alphanumerical consecutive chars
	words_lower = [w.lower() for w in words]
	return words_lower


def tfmapper():
	import os
	import sys
	for line in sys.stdin:
		for word in extract_words(line):
			print "%s\t%s\t1" % (word, os.getenv('mapreduce_map_input_file','noname'))


if __name__ == '__main__':
	tfmapper()

