hadoop jar \
     /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.0.1.jar \
     -files ./mapper.py,./reducer.py \
     -mapper tfmapper.py \
     -reducer tfreducer.py \
     -input /5000-* \
     -output /tmp