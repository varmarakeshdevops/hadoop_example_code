#!/bin/bash
echo $MAHOUT_HOME
pwd
cd $MAHOUT_HOME
# yes, this is stupid, but Mahout's shell scripts ARE stupid...
export HADOOP_HOME=/usr
pwd
./examples/bin/classify-20newsgroups.sh
