#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR && \

java -cp $DIR/target/stream-graph-proc_2.11-0.0.1-SNAPSHOT-jar-with-dependencies.jar mrow4a.spark.java.StreamGraphProcJob file://$DIR/datasets/X
