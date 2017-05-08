#!/bin/bash
hadoop fs -mkdir mahout
hadoop fs -copyFromLocal src/main/mahout/texts mahout/texts
$MAHOUT_HOME/bin/mahout seqdirectory --input mahout/texts --output mahout/texts-seq --overwrite
$MAHOUT_HOME/bin/mahout seq2sparse --input mahout/texts-seq --output mahout/texts-sparse --overwrite
$MAHOUT_HOME/bin/mahout trainnb --input mahout/texts-sparse/tfidf-vectors --output mahout/texts-train --overwrite 
$MAHOUT_HOME/bin/mahout clusterdump --input mahout/texts-train --outputFormat CSV --numWords 10