def extract_text(src_json, dstdir):
	import simplejson as json
	with open(src_json, 'r') as fp:
		tweet = json.load(fp)
		txt = tweet['text']

	dst_fpath = "{}/{}.txt".format(dstdir, tweet['id'])
	with open(dst_fpath, 'w') as fp:
		fp.write(txt)


def main(tweet_count, dpath='/home/eolus/Desktop/getRichProject/DATA_DUMP'):
	import os
	import shutil
	#dstdir = "/home/eolus/Desktop/Dauphine/bigdata/tfidf/data"
	dstdir = "/tmp/data"
	
	fnames = os.listdir(dpath)
	if tweet_count:
		fnames = fnames[:tweet_count]
	for fname in fnames[:tweet_count]:
		src_json = "{}/{}".format(dpath, fname)

		try:
			extract_text(src_json, dstdir)
		except Exception as e:
			print(e)

if __name__ == "__main__":



	import sys

	if len(sys.argv) > 1:
		n_tweets = int(sys.argv[1])
	else:
		n_tweets = None
	main(n_tweets)
