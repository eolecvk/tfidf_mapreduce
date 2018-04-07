#!/bin/bash

tf_fpath='/home/eolus/Desktop/Dauphine/bigdata/tfidf/hadoop/deploy_tf.sh'
df_fpath='/home/eolus/Desktop/Dauphine/bigdata/tfidf/hadoop/deploy_df.sh'
tfidf_fpath='/home/eolus/Desktop/Dauphine/bigdata/tfidf/hadoop/deploy_tfidf.sh'

tf_fpath && df_fpath && tfidf_fpath;

# // note:
# // add file name variable that is passed across scripts and should not conflict with existing names
# // create a wrapper that has timer (so that spark and hadoop can be measured head to head)