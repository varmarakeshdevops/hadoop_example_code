#!/bin/bash
hadoop fs -mkdir mahout
hadoop fs -copyFromLocal src/main/mahout/texts mahout/texts
$MAHOUT_HOME/bin/mahout seqdirectory --input mahout/texts --output mahout/texts-seq --overwrite
$MAHOUT_HOME/bin/mahout seq2sparse --input mahout/texts-seq --output mahout/texts-sparse --overwrite
$MAHOUT_HOME/bin/mahout kmeans -i mahout/texts-sparse/tfidf-vectors -c mahout/canopy -o mahout/kmeans -ow -k 3 --maxIter 10
$MAHOUT_HOME/bin/mahout clusterdump --input mahout/kmeans --outputFormat CSV --numWords 10