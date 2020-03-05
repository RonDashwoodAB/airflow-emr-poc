from __future__ import print_function
from operator import add
from pyspark import SparkContext


S3_BUCKET_PATH = 's3://allbirds-astronomer-airflow-poc-data'

if __name__ == "__main__":
    # Start SparkContext
    sc = SparkContext(appName="WordCount")
    # Load data from S3 bucket
    lines = sc.textFile('{}/lorem-ipsum.txt'.format(S3_BUCKET_PATH))
    # Calculate word counts
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    output = counts.collect()
    # Print word counts
    for (word, count) in output:
        print("%s: %i" % (word, count))
    # Save word counts in S3 bucket
    counts.saveAsTextFile('{}/lorem-ipsum-wordcount-output.txt'.format(S3_BUCKET_PATH))
    # Stop SparkContext
    sc.stop()