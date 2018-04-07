#!/bin/bash

TF='/home/eolus/Desktop/Dauphine/bigdata/tfidf/hadoop/deploy_tf.sh';
DF='/home/eolus/Desktop/Dauphine/bigdata/tfidf/hadoop/deploy_df.sh';
TFIDF='/home/eolus/Desktop/Dauphine/bigdata/tfidf/hadoop/deploy_tfidf.sh';
$TF && $DF && $TFIDF;