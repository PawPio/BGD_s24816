#!/bin/bash

spark-submit \
  --jars jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,jars/kafka-clients-3.4.1.jar,jars/commons-pool2-2.11.1.jar \
  main_stream.py