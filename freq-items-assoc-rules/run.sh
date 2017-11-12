#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR && \

java -cp $DIR/target/freq-items-assoc-rules_2.11-0.0.1-SNAPSHOT-jar-with-dependencies.jar mrow4a.spark.java.$1 file://$DIR/datasets/T10I4D100K.dat
