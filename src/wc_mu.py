from __future__ import print_function

__author__ = 'ph'

# Tested with python 2.7.10
import unicodedata
import array
import argparse
import sys
import os.path
import pyspark
import pyspark.sql.types as psqltypes
from pyspark import sql
from pyspark.sql import Row


class MedianArray(object):
    """
    Uses an array to compactly store a large list of repeated small
    integers for constant time add() and median() calculation operations.

    It uses the integer value as index to the array and the number of
    occurrence of that integer as the content of the cell indexed by the
    integer; e.g _ar[3] = 2L, means the number 3 has occurred twice in the
    list.

    Maintains two median pointers _w1, _w2 for calculating medians. Each
    pointer has the form (index, sub-index); e.g. a list [1, 1, 2, 2, 2, 3]
    which is stored as [0L, 2L, 3L, 1L] may have a pointer _w1 = (2,1L), which
    will point to the first 2 in the array _ar. Pointer _w2, which is at most
    one step to the right of _w1, will be _w2 = (2,2L). A pointer location,
    one step to the right of _w2, will be (2,3L). Similarly, a pointer
    location, one step to the left of _w1, will be (1, 2L).

    The flag _is_even is True when the array _ar has even number of values.
    When the array has odd number of values, _is_even is False.

    Invariants:

    a) _w2 == _w1 when _is_even == False or _ar is empty.
    b) _w2 == _w1 + 1 step, when _is_even == True and _ar is not empty.
    c) _w2 == _w1 == None when _ar is empty.
    d) _w1 and _w2 are bounded by the min and max positions of _ar.

    """

    def __init__(self, maxsize=840):
        """
        Constructor : initialize variables
        """
        self._w1, self._w2 = None, None
        self._max = maxsize + 1
        self._is_even = True
        self._ar = array.array('L', [0L] * self._max)

    # ---

    def add(self, num):
        """
        Add an element to array.
        Only integers within the range [0,maxsize] are allowed.
        """

        def right(iw):
            """
            Return the pointer location one step to the right of the input
            pointer location iw.
            """

            def next_nonzero_idx(idx):
                """
                Return an index, greater than input idx, which
                contains a nonzero value in the array.
                """

                # Performance optimization using local variable
                sar = self._ar

                for i in xrange(idx + 1, self._max):
                    if sar[i] > 0L:  # found next nonzero index
                        return i
                else:  # next nonzero index not found
                    raise RuntimeError(  # should not reach here
                        "Nonzero index larger than {0} not found.".format(idx)
                    )

            # ---

            # Unpack input pointer iw to index iidx (int) and
            # sub-index isidx (long)

            iidx, isidx = iw

            # Get the next pointer (to the right).

            if isidx < self._ar[iidx]:
                ow = (iidx, isidx + 1L)
            else:
                ow = (next_nonzero_idx(iidx), 1L)
            return ow

        # ---

        def left(iw):
            """
            Return the pointer location one step to the left of the input
            pointer location iw.
            """

            def prev_nonzero_idx(idx):
                """
                Return an index, less than input idx, which
                contains a nonzero value in the array.
                """

                # Performance optimization using local variable
                sar = self._ar

                for i in xrange(idx - 1, -1, -1):
                    if sar[i] > 0L:  # found previous nonzero index
                        return i
                else:  # previous nonzero index not found
                    raise RuntimeError(  # should not reach here
                        "Nonzero index smaller than {0} not found.".format(idx)
                    )

            # ---

            # Unpack input pointer iw to index iidx (int) and
            # sub-index isidx (long)

            iidx, isidx = iw

            # Get the previous pointer (at the left).

            if isidx > 1L:
                ow = (iidx, isidx - 1L)
            else:
                prev_idx = prev_nonzero_idx(iidx)
                ow = (prev_idx, self._ar[prev_idx])
            return ow

        # -- End of local function definitions.

        # Check if num is in the valid range.

        if 0 > num or num >= self._max:
            raise RuntimeError(
                "Value {0} is out of range [{1}, {2}].".format(
                    num, 0, self._max - 1))

        # Add num to array and toggle odd/even

        self._ar[num] += 1L
        self._is_even = not self._is_even

        # Move the median pointers _w1 and _w2. There are nine possible
        # cases (_w1 > num, _w1 == num, _w1 < num) *
        # (_w2 > num, _w2 == num, _w2 < num) out of which, three are invalid
        # (e.g. _w1[0] > num > _w2[0], _w1[0] > num == _w2[0],
        # _w1[0] == num > _w2[0]) due to the invariant _w1[0] <= _w2[0].
        # For odd number of elements in _ar, _w1 == _w2, for even number
        # of elements in _ar, _w1 < _w2.

        if self._w1 is None:   # first insertion
            self._w1, self._w2 = (num, 1L), (num, 1L)
        elif self._w1[0] < num < self._w2[0]:
            # only can happen if even number of elements in
            # array before adding num
            self._w1, self._w2 = (num, 1L), (num, 1L)
        elif self._w1[0] == num < self._w2[0]:
            # only can happen if even number of elements in
            # array before adding num
            self._w1 = (self._w1[0], self._w1[1] + 1L)
            self._w2 = self._w1
        elif self._w2[0] <= num:  # move one step to the right
            if self._is_even:
                self._w2 = (right(self._w2))
            else:
                self._w1 = self._w2
        elif self._w1[0] > num:  # move one step to the left
            if self._is_even:
                self._w1 = (left(self._w1))
            else:
                self._w2 = self._w1
        else:  # should not reach here
            raise RuntimeError("Failed to add value {0}.".format(num))

    # ---

    def median(self):
        """
        Calculate median
        """
        if self._w1 is None or self._w2 is None:
            raise RuntimeError("Cannot calculate median of an empty list.")
        elif self._is_even:
            med = float(self._w1[0] + self._w2[0])/2
        else:
            med = float(self._w1[0])
        return med


# ---- end class MedianArray()

class AnalyzeTweets(object):
    """
    Main class for analyzing tweets. Uses spark to load a file
    containing ascii tweets, and analyzes them to find word counts
    and median unique word counts.

    How to treat tweets that contain only white spaces ? This program
    will ignore empty lines, as if they did not exist in the input
    file. Consequently, the line count of the median unique words
    output file may differ from the line count of the input file,
    if input file contains blank lines.

    Non ascii characters are not expected in the input file. If
    there is a non ascii character, the program will fail.
    """

    def __init__(self, spark_context=None, rdd=None, tablename=None):
        """
        Constructor : Takes a spark context and rdd containing tweet
        text and creates an intermediate dataframe of the format
        (tweet id, word). For example, a tweet text "hello world"
        at line 5 (tweet id = 5), will result in two rows (5, "hello")
        and (5, "words").
        A tablename parameter is  used to create a temporary sparksql
        table in memory based on the dataframe for later use.
         """

        self._sc = spark_context

        # Create a SQLContext

        ssc = sql.SQLContext(spark_context)
        self._ssc = ssc

        # Add line number field by using zipWithIndex(), which, however,
        # starts line numbering at 0, so use map() to start line number
        # at 1. Also, convert each record to a Row() object with two fields
        # id = tweet id (i.e. line number), line = tweet text.

        rdd1 = rdd.zipWithIndex().map(lambda x: Row(id=x[1] + 1,
                                                    line=x[0]))

        # Split each line into words (separated by whitespace).
        # Each record in the new rdd will be a Row() with two fields
        # id = tweet id, word = a word in the tweet.

        rdd2 = rdd1.flatMap(lambda x: [Row(id=x.id, word=wrd)
                                       for wrd in x.line.split()])

        # Specify schema.

        schema = psqltypes.StructType([
            psqltypes.StructField("id", psqltypes.LongType()),
            psqltypes.StructField("word", psqltypes.StringType())])

        # Make a dataframe from the rdd.

        df1 = ssc.createDataFrame(rdd2, schema)
        df1.cache()
        
        # Register the dataframe as a temporary sparksql table.

        ssc.registerDataFrameAsTable(df1, tablename)
        
        # Save the dataframe and tablename.

        self._df = df1
        self._tblnm = tablename

    # --
    @classmethod
    def load_file(cls, sc=None, filename=None,
                  min_partitions=None, use_unicode=False):
        """
        Loads tweets from a file into a spark rdd and then
        calls the AnalyzeTweets() constructor to return an
        AnalyzeTweets object.
        """

        # Load the file to rdd. Each line of the file will be
        # loaded as a record in the rdd.

        try:
            rdd1 = sc.textFile(filename, min_partitions, use_unicode)

            # Cache to memory.

            rdd1.cache()

            # Lazy evaluation, so read the first row to make sure
            # the file can be loaded.

            rdd1.first()

        except:  # Load failed.

            print("Cannot read input file {} or file is empty.".format(
                filename), file=sys.stderr)
            raise

        else:

            # File loaded to rdd1. Now create an instance of the class by
            # calling the constructor.

            return cls(sc, rdd1, tablename="tweets")

    # --
    def print_wordcount(self, printfile=None):
        """
        Prints an ordered list of words from all the tweets along with a
        count of times each word occurred in tweets.
        """

        # Get the dataframe and tablename to local variables.

        tbl = self._tblnm
        ssc = self._ssc

        # Get the occurrence count of each word. Ignore the
        # _empty_word, if it exists.

        sql_str1 = "select word, count(*) as cnt from {0} " \
                   "group by word " \
                   "order by word".format(tbl)

        # Execute the sql.

        df2 = ssc.sql(sql_str1)
        df2.cache()

        # Get maximum length of word and count for pretty formatting.

        rdd3 = df2.map(lambda x: Row(wl=len(x.word), cl=len(str(x.cnt))))
        rdd3.cache()

        # The get_max() function compares two Rows() and gets the maximum
        # word length (wl) and count length (cl).

        def get_max(x, y):
            x.wl = max(x.wl, y.wl)
            x.cl = max(x.cl, y.cl)
            return x

        # The get_max() function is used to find max word and count length.

        width = rdd3.fold(Row(wl=0, cl=0), get_max)

        # First column word is left justified, second column count
        # is right justified.

        format_str = "{:<" + str(width.wl + 1) + "}    {:>" + \
                     str(width.cl + 1) + "}\n"

        # Print df2 to print file.

        try:
            with open(printfile, mode='w') as f:
                for r in df2.rdd.toLocalIterator():

                    """
                    # if unicode data is expected.
                    print_str = format_str.format(
                        unicodedata.normalize("NFKD", r.word).encode(
                            "ascii", "ignore"),
                        r.cnt)
                    """

                    print_str = format_str.format(r.word, r.cnt)
                    f.write(print_str)
        except:
            print("Error writing word counts to output file {}.".format(
                printfile), file=sys.stderr)

            if r is not None:
                print("word = {}, count = {}".format(
                    unicodedata.normalize("NFKD", r.word).encode("ascii",
                                                                "ignore"),
                    r.cnt), file=sys.stderr)
            raise
        finally:
            # Remove df2 and rdd3.

            df2.unpersist()
            rdd3.unpersist()

    # --
    def print_median_of_unique_words(self, printfile=None):
        """
        Calculates the number of unique words in each tweet and then
        prints a running median of unique words in each tweet to a file.
        Uses the MedianArray class to hold the unique word counts and to
        compute the running median.
        """

        # Get the table name and SQLContext to local variables

        tbl = self._tblnm
        ssc = self._ssc

        sql_str1 = "select count(distinct word) as cnt from {0} " \
                   "group by id order by id".format(tbl)

        # Execute the sql to get number of unique words for each tweet.

        df2 = ssc.sql(sql_str1)
        df2.cache()

        # Create a MedianArray() object.

        med = MedianArray()

        # Print df2 to print file.

        try:
            with open(printfile, mode='w') as f:

                # Read each row from df2 sequentially

                for r in df2.rdd.toLocalIterator():

                    # Get unique count

                    unique_count = r[0]

                    # Add the count of unique words to MedianArray

                    med.add(unique_count)

                    # Get the new median

                    new_median = med.median()

                    # Write to output file

                    print_str = "{:.2f}\n".format(new_median)
                    f.write(print_str)
        except:
            print("Error writing median unique word counts "
                  "to output file {}.".format(printfile), file=sys.stderr)
            raise
        finally:
            # Remove df2.

            df2.unpersist()


# ---- end class AnalyzeTweets()


if __name__ == "__main__":

    # Get program name (without the path).

    program_name = os.path.basename(sys.argv[0])

    # Create an argument parser.

    parser = argparse.ArgumentParser(
        description="Process an input file of tweets and "
                    "generate counts for all words, and a running median"
                    " of unique words in each tweet.",
        prog="$SPARK_HOME/bin/spark_submit {}".format(program_name),
        epilog="Specify the input filename and at least one output filename.")

    # input file is a required argument. Must specify.

    parser.add_argument("input_file",
                        help="Input filename containing the tweets. "
                             "Must specify.")

    # Optional wordcount filename.

    parser.add_argument("-w", "--wc_file",
                        help="Output filename for word counts.",
                        dest="wc_file")

    # Optional median of unique words filename.

    parser.add_argument("-m", "--mu_file",
                        help="Output filename for median of unique words.",
                        dest="mu_file")

    # Parse arguments.

    call_args = parser.parse_args()

    # wc_file and mu_file both cannot be missing.

    if call_args.wc_file is None and call_args.mu_file is None:
        parser.print_help()
        raise SystemExit("No output filename specified. Quitting.")

    # Create a spark context.

    sc1 = pyspark.SparkContext(appName=program_name)

    try:

        # Load the tweets from input file and create an AnalyzeTweets()
        # object.

        print("Loading input file.")

        at = AnalyzeTweets.load_file(sc1, call_args.input_file)

        # Print word count to an output file.

        if call_args.wc_file is not None:
            print("Finding word count.")
            at.print_wordcount(call_args.wc_file)

        # Print running median of unique words in each tweet to another
        # output file.

        if call_args.mu_file is not None:
            print("Finding median of unique words.")
            at.print_median_of_unique_words(call_args.mu_file)

    finally:

        # Stop spark.

        sc1.stop()

    print("Finished processing.")
