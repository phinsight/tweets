#!/usr/bin/env bash

# Test if SPARK_HOME is set.

if [ -n "$SPARK_HOME" ]; then
   echo "SPARK_HOME set to $SPARK_HOME"
else
   echo "SPARK_HOME is not set." 
   echo "Please set SPARK_HOME to a valid spark home directory."
   exit 1
fi

# Get the source directory of this file.
DIR="$(cd -P "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Change the directory to source directory.
cd $DIR

# Run the wc_mu.py python script with spark.
# Tested with Spark 1.3.1 and 1.4.

exec $SPARK_HOME/bin/spark-submit --master "local[*]" \
        src/wc_mu.py \
	tweet_input/tweets.txt \
   	-w tweet_output/ft1.txt \
        -m tweet_output/ft2.txt
