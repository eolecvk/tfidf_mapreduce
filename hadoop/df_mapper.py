#!/usr/bin/python2

def dfmapper():
	import sys
	import os
	for line in sys.stdin:
		print "%s\t1" % line.strip()

if __name__ == '__main__':
	dfmapper()

