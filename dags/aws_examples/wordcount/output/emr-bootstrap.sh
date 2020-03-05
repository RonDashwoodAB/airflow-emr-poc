#!/bin/bash

s3_bucket="$1"
s3_bucket_path="$s3_bucket/wordcount.tar.gz"

# Download the S3-hosted job
aws s3 cp $s3_bucket_path /home/hadoop/wordcount.tar.gz

mkdir -p /home/hadoop/wordcount

# Untar the S3-hosted job
tar zxvf "/home/hadoop/wordcount.tar.gz" -C /home/hadoop/wordcount