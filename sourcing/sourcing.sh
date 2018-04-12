#!/bin/bash

DATA_DUMP='/home/eolus/Desktop/getRichProject/DATA_DUMP';

# Pass number of tweets in sourcing script args
NUM_TWEETS=$1

for filename in /Data/*.txt; do
    for ((i=0; i<=$NUM_TWEETS; i++)); do
        ./MyProgram.exe "$filename" "Logs/$(basename "$filename" .txt)_Log$i.txt"
    done
done