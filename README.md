Tweet Analysis with pySpark
=======================

A python program **wc_mu.py**, that reads ascii tweets from a
file tweets.txt in the **tweet_input** directory and
1. Calculates the total number of times each word has been tweeted.
2. Calculates the median number of unique words per tweet, and updates this median for each tweet (running median).

The program creates two files in the **tweet_output** directory :
1. **ft1.txt** : Holds the data about the total number of times each word has been tweeted.
2. **ft2.txt** : Holds the data about the median number of unique words per tweet.

The program uses Apache Spark and python to do the processing in a scalable way.  It has been tested with Spark 1.4 / 1.3.1 and python 2.7.10 and 2.7.6.

A bash shell script, **run.sh**, is provided to invoke the wc_mu.py with the required arguments.

----------


Requirements
-------------

1.  A functional version of Apache Spark 1.4 or 1.3.1 which can be installed from https://spark.apache.org/downloads.html . The base installation directory for Spark (**SPARK_HOME**) should be set in the bash shell (e.g. with the command):
***export SPARK_HOME=/user/local/bin/spark1.4***
2. Spark will need the JVM 6, 7 or 8 to run (tested with 6 and 7).
3.  Python 2.7 (tested with 2.7.6 and 2.7.10).
4.  GNU bash (tested with version 3.2 and 4.3). 


> **Python Libraries Used:**

> - unicodedata
> - array
> - argparse
> - sys
> - os.path
> - pyspark

