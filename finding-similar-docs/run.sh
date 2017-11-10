#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR && \

tar -zxvf datasets/mini_newsgroups.tar.gz -C datasets

java -cp $DIR/target/finding-similar-docs_2.11-0.0.1-SNAPSHOT-jar-with-dependencies.jar mrow4a.spark.java.$1 file://$DIR/datasets/mini_newsgroups/alt.atheism/*
