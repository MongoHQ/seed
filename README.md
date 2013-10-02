seed
====
Seed is a tool that allows the easy syncing of a database(s) from one replica set to another.  It's a work in progress, and has been known to be finicky, but is generally fairly good at what it does.

When to use Seed
================
- when you want a zero downtime migration from replica set to replica set
- when you want to change the database name during the migration
- you want to migrate data to tokutek

What it does
============
- connect to both the source and the destination and finds the oplog timestamp
- copies unique indexes from source to destination (changing their namespace)
- copies users 
- copies all the collections in parallel
- copies non-unique indexes
- tails the oplog from the initial timestamp, and applies the operations in a batch (ignoring a list of blacklisted commands, dropDatabase, etc).  There is no conflict resolution in seed.  When writing to the source and the destination, the last write always wins.

How to get it
=============
- `git clone https://github.com/MongoHQ/seed.git`
- `cd seed && go build`

How to run it
=============
Seed has a bunch of command line options, but in general you can run seed something like this:
`./bin/seed -s mongodb://user:<password>/hostname.com/DatabaseName -d mongodb://user:<password>@hostname2:27018/Otherdatabase -o -i`
- `-s`, `-d` are the source and desgination uri's respectively
- `-o` asks seed to tail the oplog
- `-i` asks seed to perform an initial copy

other options
=============
- `-allDbs=false`: copy all the databases from the source to the destination
- `-forceTableScan`=false: don't sort by ids in initial sync. (sorting by _id can sometimes miss documents if _id is a non ObjectId())
- `-forceindex`="": indexing concerns can be tough.  by default, seed will copy the indexes as they were built, (foreground, background, etc).  this option forces the indexes to be built either in the foreground or in the background.  if you supply the value 'immediate', then seed will apply both unique and non-unique indexes before it copies any data.
- `-from`, `-to`: when used with the -o option will set the start and stop timestamp.  you can use formats like -from=now -to=inf, or -from=1234567,123 etc
- `-oplog`: if you are syncing from a machine with a master-slave setup, then the oplog should be set as -oplog=oplog.\$main 
- `-stats`: show period mongo stats as the sync progresses
- `-v`, `-vv` various levels of debug information

About MongoHQ
=============
[MongoHQ](https://www.mongohq.com/) is a fully-managed platform used by developers to deploy, host, and scale open-source databases.


