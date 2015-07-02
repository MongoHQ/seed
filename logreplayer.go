package main

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type logReplayer struct {
	source *mgo.Session
	target Target
	from   timestamp
	to     timestamp
	srcDB  string
	dstDB  string
}

func newLogReplayer(source *mgo.Session, target Target, from, to timestamp, srcdb, dstdb string) (*logReplayer, error) {
	if err := target.KeepAlive(); err != nil {
		return nil, err
	}

	return &logReplayer{
		source: source,
		target: target,
		from:   from,
		to:     to,
		srcDB:  srcdb,
		dstDB:  dstdb,
	}, nil
}

func (l *logReplayer) Close() {
	l.source.Close()
	l.target.Close()
}

// this is where most of the magic happens
// we tail the oplog, collect ops into a buffer
// and flush the buffer to the destination when it's full
// we take a timeout to breath every once in a while, and ping the
// destination, reconnect if necasary
// and re-execute the oplog tail if it becomes stale
func (l *logReplayer) playLog() (err error) {

	var (
		lastTimestamp     = bson.MongoTimestamp(l.from) // set his is a de
		currentOp         = OplogDoc{}
		sourceOplog       = l.source.DB("local").C(*oplog_name)
		ch                = make(chan OplogDoc, BUFFER_SIZE)
		cherr             = make(chan error, BUFFER_SIZE)
		chsig             = make(chan os.Signal, 1)
		errorCount    int = 0
		count         int = 0
		total         int = 0
	)

	signal.Notify(chsig, os.Interrupt, syscall.SIGTERM)

	// this runner will pull the incoming ops off the channel and apply them
	go func(ch chan OplogDoc, cherr chan error) {
		for op := range ch {
			err := l.target.ApplyOne(op)
			if err != nil {
				logger.Debug("ApplyOp got an error, retrying in 10 seconds")
				l.reconnectDest()
				err = l.target.ApplyOne(op)
			}
			cherr <- err
		}
	}(ch, cherr)

	// set up initial query to the oplog
	logger.Info("Replaying oplog from %s to %s", l.from, l.to)
	iter := sourceOplog.Find(bson.M{"ts": bson.M{"$gt": bson.MongoTimestamp(l.from)}}).LogReplay().Sort("$natural").Tail(1 * time.Second)

	// loop over the oplog
outer:
	for {
		for iter.Next(&currentOp) {

			errorCount = 0
			lastTimestamp = currentOp.Ts

			// are we quitting? did we catch a signal?
			if quitting(chsig) {
				logger.Info("Recieved Sigint, quitting")
				break outer
			}

			// if there are replies in queue, then drain them
			more := true
			for more {
				select { // suck any responses off the result channel
				case err = <-cherr:
					count--
					if err != nil {
						logger.Debug("bailing out.  Error %v.", err)
						break outer
					}
				default:
					more = false
				}
			}

			if currentOp.Kind() == NOOP {
				logger.Finest("noop, skipping %s", currentOp.String())
				continue
			}

			if currentOp.Kind() == COMMAND {
				blacklisted, comm := isBlacklistedCommand(currentOp)
				if blacklisted {
					logger.Debug("blacklisted command %s, skipping", comm)
					continue
				}
			}

			// match our namespace, or skip this op
			if *allDbs || strings.HasPrefix(currentOp.Ns, l.srcDB) {

				currentOp.Ns = strings.Replace(currentOp.Ns, l.srcDB, l.dstDB, 1)
				if currentOp.Kind() == COMMAND || (currentOp.isSystemCollection() && currentOp.Kind() == INSERT) {
					ns, exists := currentOp.O["ns"]
					if exists {
						currentOp.O["ns"] = strings.Replace(ns.(string), l.srcDB, l.dstDB, 1)
					}
				}

				count++
				total++
				ch <- currentOp

				dumpProgress(total, count, timestamp(currentOp.Ts).diff(), timestamp(currentOp.Ts)) // log progress

				// we're as far as we want to go, time to clean up and go
				if timestamp(currentOp.Ts) >= l.to {
					break outer
				}
				currentOp = OplogDoc{}
			} else {
				logger.Finest("skipping op %s", currentOp.String())
			}
		}
		// check to see if we're expired, i.e. if now is > our specified end point
		now := NewTimestamp("now")
		if now >= l.to {
			iter.Close()
			break outer
		}

		// are we quitting? did we catch a signal?
		if quitting(chsig) {
			logger.Info("Recieved Sigint, quitting")
			break outer
		}

		if iter.Timeout() {
			// we can continue on the same connection without requerying.
			logger.Debug("No data for 1 second")
			continue
		}

		logger.Debug("Error reading from source, retrying in 10 seconds")
		// take a break, ping the destination mongo to make sure all is well
		l.reconnectDest()

		// re-query
		errorCount++
		if errorCount > 10 {
			logger.Critical("too many errors reading from oplog, bailing.")
			os.Exit(1)
		}
		iter = sourceOplog.Find(bson.M{"ts": bson.M{"$gt": lastTimestamp}}).LogReplay().Sort("$natural").Tail(1 * time.Second)
	}

	for count > 0 {
		err = <-cherr
		count--
		if err != nil {
			logger.Debug("apply op got an error %v", err)
		}
	}

	return nil
}

// reconnect to the destination
func (l *logReplayer) reconnectDest() {
	time.Sleep(10 * time.Second)
	if err := l.target.KeepAlive(); err != nil {
		logger.Critical("Cannot reconnect to destination, quitting, %v", err)
		os.Exit(1)
	}
}

// dump log progress, depending on how far away from the target we are
func dumpProgress(total, count int, diff int64, ts timestamp) {
	var mod int
	switch {
	case diff > 300:
		mod = 1000
	case diff > 60:
		mod = 100
	default:
		mod = 10
	}
	if total%mod == 0 {
		logger.Info("Queued %d ops.  backlog: %d, last ts: %s, (%d sec behind)", total, count, ts, diff)
	}
}

func quitting(chsig chan os.Signal) bool {
	select {
	case <-chsig:
		return true
	default:
		return false
	}
}

// some commands we don't want to replicate over, syncing over a copydb, dropDatabase or replSetINitiate seems like a bad idea
var blacklistedCommands = []string{"dropDatabase", "copydb", "replSetFreeze", "replSetInitiate", "replSetMaintenance", "replSetReconfig", "replSetStepDown", "replSetSyncFrom", "resync"}

func isBlacklistedCommand(doc OplogDoc) (bool, string) {
	var ok bool
	for _, v := range blacklistedCommands {
		_, ok = doc.O[v]
		if ok {
			logger.Debug("%+v", doc.O)
			return true, v
		}
	}
	return false, ""
}
