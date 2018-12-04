#!/usr/bin/env bash

# identify the location of the script
rootdir=`dirname $0`  # may be relative path
rootdir=`cd $rootdir && pwd`  # ensure absolute path
rootdir=`dirname $rootdir`  # get parent directory

for f in *.parquet
do
   base=`echo $f | sed -e 's/.parquet//'`
   echo $base
   if [[ ! -f ${base}.csv ]]
   then
      cmd="java -cp $rootdir/target/os-models-1.0-SNAPSHOT-all.jar com.cdd.bin.ParquetToCsv $f $base.csv"
      echo $cmd
      eval $cmd
   fi
done
