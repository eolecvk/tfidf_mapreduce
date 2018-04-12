FPATH_HDJAR='/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.0.1.jar'
FPATH_TFIDF_MAPPER='/home/eolus/Desktop/Dauphine/bigdata/tfidf/hadoop/tfidf_mapper.py'
FPATH_TFIDF_REDUCER='/home/eolus/Desktop/Dauphine/bigdata/tfidf/hadoop/tfidf_reducer.py'
FPATH_INPUT='/tmp/tfdf_data_0'
FPATH_OUTPUT='/tmp/tfidf_data_0'

rm -rf $FPATH_OUTPUT

/usr/local/hadoop/bin/hadoop jar \
     $FPATH_HDJAR \
     -mapper $FPATH_TFIDF_MAPPER -reducer $FPATH_TFIDF_REDUCER \
     -input $FPATH_INPUT \
     -output $FPATH_OUTPUT