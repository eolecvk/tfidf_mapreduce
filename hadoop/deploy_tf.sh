#!/bin/bash
FPATH_HDJAR='/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.0.1.jar'
FPATH_TF_MAPPER='/home/eolus/Desktop/Dauphine/bigdata/tfidf/hadoop/tf_mapper.py'
FPATH_TF_REDUCER='/home/eolus/Desktop/Dauphine/bigdata/tfidf/hadoop/tf_reducer.py'


#FPATH_INPUT='/home/eolus/Desktop/Dauphine/bigdata/tfidf/data'
FPATH_INPUT='/tmp/data'
FPATH_OUTPUT='/tmp/tf_data_0'

rm -rf $FPATH_OUTPUT

/usr/local/hadoop/bin/hadoop jar \
     $FPATH_HDJAR \
     -mapper $FPATH_TF_MAPPER \
     -reducer $FPATH_TF_REDUCER \
     -input $FPATH_INPUT \
     -output $FPATH_OUTPUT
