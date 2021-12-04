#!/usr/bin/python3
##
# Demos using an aggregation pipeline to calculate the average value of a field in the documents of
# a collection in a MongoDB database, but splitting the workload up into multiple aggregation
# pipeline jobs executed in parallel against subsets of documents to show how the total response
# time for the workload can be reduced.
#
# For usage first ensure the '.py' script is executable and then run:
#  $ ./mongo-parallel-agg.py -h
#
# Example:
#  $ ./mongo-parallel-agg.py -d "geodb" -c "countries" -s 8 -p "name" -a "population"
#
# Prerequisites:
# * Install PyMongo driver, eg:
#  $ pip3 install --user pymongo
##
import sys
from argparse import ArgumentParser
from datetime import datetime
from pprint import pprint
from datetime import datetime
from collections import namedtuple
from functools import reduce
from multiprocessing import Process, Manager
from pymongo import MongoClient, ASCENDING
from pymongo.errors import OperationFailure
from bson.min_key import MinKey
from bson.max_key import MaxKey


# Named tuples to capture the main request and response parameters for each split aggregation job
AggAvgBatchJob = namedtuple("AggAvgBatchJob", ["filterField", "filterLower", "filterUpper",
                            "avgField"])
AggAvgBatchResult = namedtuple("AggAvgBatchResult", ["total", "count"])


##
# Main function to parse passed-in process before invoking the core processing function
##
def main():
    argparser = ArgumentParser(description="Demos using an aggregation pipeline to calculate the "
                                           "average value of a field in the documents of a "
                                           "collection in a MongoDB database, but splitting the "
                                           "workload up into multiple aggregation pipeline jobs "
                                           "executed in parallel against subsets of documents to "
                                           "show how the total response time for the workload can "
                                           "be reduced.")
    argparser.add_argument("-m", "--url", default=DEFAULT_MONGODB_URL,
                           help=f"MongoDB cluster URL (default: {DEFAULT_MONGODB_URL})")
    argparser.add_argument("-d", "--db", default=DEFAULT_DBNAME,
                           help=f"Database name (default: {DEFAULT_DBNAME})")
    argparser.add_argument("-c", "--coll", default=DEFAULT_COLLNAME,
                           help=f"Collection name (default: {DEFAULT_COLLNAME})")
    argparser.add_argument("-s", "--splitsAmount", default=DEFAULT_SPLITS_AMOUNT, type=int,
                           help=f"The number of division points the data-set should be split up by "
                                f"for parallel processing - the actual number of spawned "
                                f"sub-processing jobs will be one greater than this value - specify"
                                f" zero for no dividing of the data and no parallel processing "
                                f"(default: {DEFAULT_SPLITS_AMOUNT})")
    argparser.add_argument("-p", "--partitionField", default=DEFAULT_PARTITION_FIELD,
                           help=f"Name of the field in each document to partition (split) the "
                                f"aggregation workload on - ensure this will as much or more "
                                f"granularity as the '--splitsAmount' argument you have provided "
                                f"(default: {DEFAULT_PARTITION_FIELD})")
    argparser.add_argument("-a", "--fieldToAverage", default=DEFAULT_FIELD_TO_AVERAGE,
                           help=f"Name of the number field to calculate an average for "
                                f"(default: {DEFAULT_FIELD_TO_AVERAGE})")
    args = argparser.parse_args()
    print()
    run(args.url, args.db, args.coll, args.splitsAmount, args.partitionField, args.fieldToAverage)


##
# The main function for performing the average aggregation pipeline against a data set (either as
# one single aggregation job or a set of split aggregation jobs in parallel)
##
def run(url, dbname, collName, splitsAmount, partitionField, avgField):
    print(f"\nConnecting to MongoDB using URL '{url}' "
          f"({datetime.now().strftime(DATE_TIME_FORMAT)})\n")
    connection = MongoClient(url)
    db = connection[dbname]
    print(f" Ensuring a compound index on fields '{partitionField}' and '{avgField}' exists to "
          f"support covered queries/aggregations (may take a while if it doesn't exist)")
    db[collName].create_index([(partitionField, ASCENDING), (avgField, ASCENDING)])
    print(f" Determining type for partition field '{partitionField}'")
    type = getFieldType(db, collName, partitionField)
    average = -1

    if type == "missing":
        sys.exit(f"\nERROR: Specified field '{partitionField}' does not exist in any documents: "
                 f"'{type}'")
    elif type not in ["string", "date", "int", "double", "long", "timestamp", "decimal", "bool"]:
        sys.exit(f"\nERROR: Specified field '{partitionField}' has a type which is not useful for "
                 f"performing parallel processing on (type is: '{type}'")

    # Run just a single aggregation pipeline against the whole data-set
    if splitsAmount <= 0:
        print(f" Processing one full aggregation serially")
        start = datetime.now()
        average = executeFullAggPipeline(db, collName, avgField)
    # Run multiple split aggregation pipelines in parallel
    else:
        # Analyse the data set to work out an even spread of split points on the field to partition
        print(f" Determining split points for partition field '{partitionField}'")
        splitPoints = getFieldSplitPoints(db, collName, partitionField, splitsAmount)
        aggAvgBatchJobs = assembleBatchJobSpecs(splitPoints, partitionField, avgField)
        print(f" Processing split aggregations in parallel ({len(aggAvgBatchJobs)} processes)")
        print(" |-> ", end="", flush=True)
        start = datetime.now()
        # Execute multiple aggs in parallel (need to calculate totals & counts rather than average)
        results = spawnBatchProcesses(aggAvgBatchJobs, executeSplitAggPipeline, url, dbname,
                                      collName)
        print()
        # Bring together the result from each aggregation
        total = reduce(lambda curr, next: curr + next, [result.total for result in results])
        count = reduce(lambda curr, next: curr + next, [result.count for result in results])
        # print(f"total={total}, count={count}")
        average = total / count

    end = datetime.now()
    print(f"\nAverage: {average}")
    print(f"\nFinished database processing work in {int((end-start).total_seconds())} seconds")
    print()


###
# Execute a single aggregation against the whole data-set (don't split into multiple jobs).
##
def executeFullAggPipeline(db, collName, avgField):
    pipeline = [
        {"$group": {
            "_id": "",
            "average": {"$avg": f"${avgField}"},
        }},
    ]

    firstRecord = db[collName].aggregate(pipeline).next()
    return firstRecord['average']


##
# Execute a MongoDB a in its own OS process (hence must re-establish a MongoClient connection
# for each process because PyMongo connections can't be shared across processes). Also any results
# 'returned' directly by this function would be lost, due to the way multi-processing in Python is
# implemented, therefore the result of the function is attached to the 'resultsList' parameter which
# was passed in.
#
# This function runs the aggregation pipeline against a subset of data where the partition field is
# between an lower and upper value.
##
def executeSplitAggPipeline(url, dbname, collName, aggAvgBatchJob, resultsList):
    print("[", end="", flush=True)
    connection = MongoClient(url)
    db = connection[dbname]
    result = None

    pipeline = [
        {"$match": {
            aggAvgBatchJob.filterField: {
                "$gte": aggAvgBatchJob.filterLower,
                "$lt": aggAvgBatchJob.filterUpper,
            }
        }},

        {"$group": {
            "_id": "",
            "total": {"$sum": f"${aggAvgBatchJob.avgField}"},
            "count": {"$sum": {"$cond": {
                        "if": {"$eq": [f"${aggAvgBatchJob.avgField}", None]},
                        "then": 0,
                        "else": 1
            }}},
        }},
    ]

    try:
        firstRecord = db[collName].aggregate(pipeline).next()
        result = AggAvgBatchResult(firstRecord['total'], firstRecord['count'])
    except StopIteration:
        result = AggAvgBatchResult(0, 0)

    print("]", end="", flush=True)
    resultsList.append(result)


#
# Find out the main type of the field to be partitioned.
##
def getFieldType(db, collName, fieldName):
    pipeline = [
        {"$sample": {
            "size": 100
        }},

        {"$project": {
            "type": {"$type": f"${fieldName}"},
        }},

        {"$group": {
            "_id": "$type",
            "count": {"$sum": 1},
        }},

        {"$set": {
            "type": "$_id",
            "_id": "$$REMOVE",
        }},

        {"$sort": {
            "count": -1,
        }},

        {"$limit": 1},
    ]

    firstRecord = db[collName].aggregate(pipeline).next()
    return firstRecord[BSON_TYPE_FIELD]


##
# Analyse the data set to work out an even spread of split points on the field to partition.
##
def getFieldSplitPoints(db, collName, fieldName, splitsAmount):
    pipeline = [
        {"$sample": {
            "size": 50000
        }},

        {"$bucketAuto": {
            "groupBy": f"${fieldName}",
            "buckets": splitsAmount
        }},

        {"$group": {
            "_id": "",
            "splitPoints": {
                "$push": "$_id.min",
            },
        }},

        {"$unset": [
            "_id",
        ]},
    ]

    firstRecord = db[collName].aggregate(pipeline).next()
    return firstRecord["splitPoints"]


##
# Assembles the list of batch jobs that will need to be run.
##
def assembleBatchJobSpecs(splitPoints, partitionField, avgField):
    splitPoints = splitPoints.copy()
    currItem = MinKey
    splitPoints.append(MaxKey)
    aggAvgBatchJobs = []

    for nextItem in splitPoints:
        aggAvgBatchJobs.append(AggAvgBatchJob(partitionField, currItem, nextItem, avgField))
        currItem = nextItem

    return aggAvgBatchJobs


##
# Spawn multiple process, each running a piece of work in parallel against a batch of records from
# a source collection. Results from each process should be added to the 'procSharedResultsList'
# parameters.
#
# The 'funcToParallelise' argument should have the following signature:
#     myfunc(*args, batch, procSharedResultsList)
# E.g.:
#     myfunc(url, dbName, collName, batchJobSpec, procSharedResultsList)
##
def spawnBatchProcesses(batches, funcToParallelise, *args):
    resultsList = []

    with Manager() as manager:
        # Get handle on an object that can be added to by each sub-process & read by parent process
        procSharedResultsList = manager.list()
        processesList = []

        # Create a set of OS processes to perform each batch job in parallel
        for batch in batches:
            process = Process(target=wrapperProcessWithKeyboardException, args=(funcToParallelise,
                              *args, batch, procSharedResultsList))
            processesList.append(process)

        try:
            # Start all processes
            for process in processesList:
                process.start()

            # Wait for all processes to finish
            for process in processesList:
                process.join()

            # Populated normal list of results from the special shared process results list
            for result in procSharedResultsList:
                resultsList.append(result)
        except KeyboardInterrupt:
            print(f"\nKeyboard interrupted received\n")
            shutdown()

    return resultsList


##
# For a newly spawned process, wraps a business function with the catch of a keyboard interrupt to
# then immediately ends the process when the exception occurs without spitting out verbiage.
##
def wrapperProcessWithKeyboardException(*args):
    try:
        args[0](*(args[1:]))
    except OperationFailure as err:
        print("\n\nError occurred when attempting to execute a MongoDB operation. Error Details:")
        print(err)
        sys.exit(0)
    except KeyboardInterrupt:
        sys.exit(0)


##
# Swallow the verbiage that is spat out when using 'Ctrl-C' to kill the script.
##
def shutdown():
    try:
        sys.exit(0)
    except SystemExit as e:
        os._exit(0)


# Constants
DEFAULT_MONGODB_URL = "mongodb://localhost:27017"
DEFAULT_DBNAME = "sample_mflix"
DEFAULT_COLLNAME = "movies"
DEFAULT_SPLITS_AMOUNT = 32
DEFAULT_PARTITION_FIELD = "title"
DEFAULT_FIELD_TO_AVERAGE = "metacritic"
BSON_TYPE_FIELD = "type"
DATE_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


##
# Main
##
if __name__ == "__main__":
    main()
