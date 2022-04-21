# FeedFile
PySpark script to read a feed file &amp; handle diffrent exceptions

PURPOSE ::: The purpose of the py script is to read the feed file from hdfs location and save the data into a table in append mode
OPTIMIZATION ::: Couple of optimization techniques has been implemented with regards to Spark.

                 1) LOGGER to catch only the desired msgs on LOG file as a WARNing
                 2) kyro serialiser has been used to submit the Job on Cluster/Client

EXCEPTION HANDLING :::
                  1) Handling the code execution smoothly, in case of feed file unavailability

