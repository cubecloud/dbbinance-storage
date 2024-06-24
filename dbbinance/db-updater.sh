#!/bin/bash

cd $1

files=$(ls *.env)

set -o allexport
for i in $files
do
	source $i
	echo "$i - exported"
done
set +o allexport

echo "Run dbdata-updater" 
python $1dbdata_updater.py

