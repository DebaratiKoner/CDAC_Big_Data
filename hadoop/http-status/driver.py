#!/usr/bin/env python3

import subprocess

STREAMING_JAR = "/usr/local/hadoop/hadoop-3.3.6/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar"

INPUT_PATH = "/user/vboxuser/http_status/input"
OUTPUT_PATH = "/user/vboxuser/http_status/output"

command = [
    "hadoop",
    "jar",
    STREAMING_JAR,

    # Generic Hadoop options MUST come before streaming options
    "-D", "mapreduce.job.reduces=2",
    "-D", "stream.num.map.output.key.fields=2",
    "-D", "mapreduce.partition.keypartitioner.options=-k1,1",

    # Streaming options
    "-input", INPUT_PATH,
    "-output", OUTPUT_PATH,

    "-mapper", "mapper.py",
    "-combiner", "combiner.py",
    "-reducer", "reducer.py",

    "-partitioner",
    "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner",

    "-file", "mapper.py",
    "-file", "combiner.py",
    "-file", "reducer.py"
]

print("Starting Hadoop MapReduce job...")

subprocess.run(command, check=True)

print("Job completed successfully.")
