# Mongo Parallel Agg

The _mongo-parallel-agg_ tool demos using an aggregation pipeline to calculate the average value of a field in the documents of a collection in a MongoDB database, but splitting the workload up into multiple aggregation pipeline jobs executed in parallel against subsets of documents to show how the total response time for the workload can be reduced.

The _mongo-parallel-agg_ tool relies on analysing an inflated version of the [Atlas sample data set](https://docs.atlas.mongodb.com/sample-data/)) and specifically the *samples_mflix* _movies_ data. This doesn't mean the target cluster has to be in [Atlas](https://www.mongodb.com/cloud), it is just typically easier that way. Alternatively you can [download your own copy of the sample data set](https://www.mongodb.com/developer/article/atlas-sample-datasets/) and load it into your self-managed MongoDB database cluster. The instructions here will assume you are using an Atlas cluster.

The _mongo-parallel-agg_ tool also leverages the [mongo-mangler](https://github.com/pkdone/mongo-mangler) utility to expand the sample _movies_ data-set to a collection of 100 million records, using duplication.

For more information on using this tool to demonstrate decreasing the see execution time of an aggregation pipeline, see the blog post [TODO](TODO).


## Steps To Run

 1. Ensure you have a running MongoDB Atlas cluster (e.g. an M40-tier 2-shard cluster) deployed and which is network accessible from your client workstation (by configuring an [IP Access List entry](https://docs.atlas.mongodb.com/security/ip-access-list/)).

 2. Ensure you are connecting to the MongoDB cluster with a database user which has __read privileges for the source database and read + write privileges the target database__. If you are running a __Sharded cluster__, the database user must also have the __privileges to run the 'enablingSharding' and 'splitChunk' commands__. In Atlas, you would typically need to assign the 'Atlas Admin' role to the database user to achieve this.

 3. On your client workstation, ensure you have Python 3 (version 3.8 or greater) and the MongoDB Python Driver ([PyMongo](https://docs.mongodb.com/drivers/pymongo/)) installed. Example to install _PyMongo_:

```console
pip3 install --user pymongo
```
 4. Download the [mongo-mangler](https://github.com/pkdone/mongo-mangler) project and unpack it on your local workstation, ready to use, and ensure the `mongo-mangler.py` file is executable.

 5. For your Atlas Cluster in the [Atlas Console](https://cloud.mongodb.com/), choose the option to **Load Sample Dataset**. 
 
 6. In a terminal, change directory to the unpacked **mongo-mangler** root folder and execute the following to connect to the Atlas cluster and copy and expand the data from an the existing `sample_mflix.movies` collection to an a new collection, `testdb.big_collection`, to contain 10 million documents (if the target is a sharded cluster this will automatically define a range shard key on the _field_ with pre-split chunks):

```console
./mongo-mangler.py -m "mongodb+srv://myusr:mypwd@mycluster.abc1.mongodb.net/" -d "sample_mflix" -c "movies" -t "movies_big" -k "title" -s 100000000
```

&nbsp;&nbsp;&nbsp;&nbsp;_NOTE_: Before executing the above command, first change the URL's _username_, _password_, and _hostname_ to match the URL of your running Atlas cluster.

 7. From the terminal, change directory to the root folder of this **mongo-parallel-agg** project and execute the following to connect to the Atlas cluster and execute a simple aggregation pipeline, split into 16 sub-pipelines run in parallel, which calculates and then prints out the average "metacritic" score across all movies in the collection of 100 million records, including printing out the total execution time at the end of the run:

```console
./mongo-parallel-agg.py -m "mongodb+srv://myusr:mypwd@mycluster.abc1.mongodb.net/" -d "sample_mflix" -c "movies_big" -s 16 -p "title" -a "metacritic"
```

&nbsp;&nbsp;&nbsp;&nbsp;_NOTE_: Before executing the above command, first change the URL's _username_, _password_, and _hostname_ to match the URL of your running Atlas cluster. Also, optionally change the _-s_ parameter to a different value to run a different number of sub-processes. You can set this value to _1_ to instruct the tool to not split up the aggregation pipeline and instead run the full aggregation in one go from the main single process.

