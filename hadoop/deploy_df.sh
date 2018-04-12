FPATH_HDJAR='/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.0.1.jar'
FPATH_DF_MAPPER='/home/eolus/Desktop/Dauphine/bigdata/tfidf/hadoop/df_mapper.py'
FPATH_DF_REDUCER='/home/eolus/Desktop/Dauphine/bigdata/tfidf/hadoop/df_reducer.py'
FPATH_INPUT='/tmp/tf_data_0'
FPATH_OUTPUT='/tmp/tfdf_data_0'

rm -rf $FPATH_OUTPUT

/usr/local/hadoop/bin/hadoop jar \
     $FPATH_HDJAR \
     -mapper $FPATH_DF_MAPPER \
     -reducer $FPATH_DF_REDUCER \
     -input $FPATH_INPUT \
     -output $FPATH_OUTPUT