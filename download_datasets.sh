#!/bin/sh
wget http://files.grouplens.org/datasets/movielens/ml-latest.zip
wget http://files.grouplens.org/datasets/movielens/ml-latest-small.zip

unzip -o "ml-latest.zip"
unzip -o "ml-latest-small.zip"
mkdir -p "./datasets"
mv ml-latest "./datasets"
mv ml-latest-small "./datasets"
