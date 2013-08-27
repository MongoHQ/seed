package main

import (
	"fmt"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"os"
	"strings"
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

func (l *logReplayer) writeBuffer(buffer []OplogDoc) error {
	if len(buffer) == 0 {
		return nil
	}

	opResult, err := l.target.Apply(buffer)
	if err != nil {
		logger.Debug("Source errored with %v, sleeping and reconnecting", err)
		time.Sleep(60 * time.Second)
		if err = l.target.KeepAlive(); err != nil {
			return fmt.Errorf("Cannot reconnect to source, %v", err)
		}
		opResult, err = l.target.Apply(buffer)
		if err != nil {
			logger.Info("%+v", buffer)
			return err
		}
	}

	if opResult.Ok != 1 {
		return fmt.Errorf("could not apply full buffer (%d/%d)", opResult.Applied, len(buffer))
	}

	logger.Debug("%s - applied (%d/%d) ops", timestamp(buffer[len(buffer)-1].Ts), opResult.Applied, len(buffer))
	return nil
}

// this is where most of the magic happens
// we tail the oplog, collect ops into a buffer
// and flush the buffer to the destination when it's full
// we take a timeout to breath every once in a while, and ping the
// destination, reconnect if necasary
// and re-execute the oplog tail if it becomes stale
func (l *logReplayer) playLog() error {

	var (
		lastTimestamp bson.MongoTimestamp
		currentOp     = OplogDoc{}
		buffer        = make([]OplogDoc, 0, BUFFER_SIZE)
		bufferSize    = 0
		sourceOplog   = l.source.DB("local").C(*oplog_name)
	)

	// set up initial query to the oplog
	logger.Info("Replaying oplog from %s to %s", l.from, l.to)
	iter := sourceOplog.Find(bson.M{"ts": bson.M{"$gt": bson.MongoTimestamp(l.from)}}).Sort("$natural").Tail(1 * time.Second)

	// loop over the oplog
outer:
	for {
		for iter.Next(&currentOp) {
			lastTimestamp = currentOp.Ts

			if currentOp.Kind() == NOOP {
				logger.Finest("noop, skipping %s", currentOp.String())
				continue
			}

			if currentOp.Kind() == COMMAND {
				if isBlacklistedCommand(currentOp) {
					logger.Debug("blacklisted command, skipping")
					continue
				}
			}

			if timestamp(currentOp.Ts) >= l.to {
				// we're as far as we want to go, time to clean up and go
				if err := l.writeBuffer(buffer); err != nil {
					return fmt.Errorf("Err: %s, Quitting", err)
				}
				iter.Close()
				break outer
			}

			// match our namespace, or skip this op
			if *allDbs || strings.HasPrefix(currentOp.Ns, l.srcDB) {
				currentOp.Ns = strings.Replace(currentOp.Ns, l.srcDB, l.dstDB, 1)
				if strings.HasSuffix(currentOp.Ns, ".system.indexes") {
					// index inserts need special treatmen!
					ns, ok := currentOp.O["ns"]
					if ok {
						currentOp.O["ns"] = strings.Replace(ns.(string), l.srcDB, l.dstDB, 1)
					}
				}
				// crucial
				// if we have room in the buffer, then add the doc
				// otherwise, send it off to be written
				sz, er := docSize(currentOp)
				if er != nil {
					logger.Error("can't find doc size, %v", er)
					return er
				}
				// this document would put us over the edge, too big.  so lets write
				if ((sz + bufferSize) > MAX_BUFFER_SIZE) || (len(buffer) == cap(buffer)) {
					if err := l.writeBuffer(buffer); err != nil {
						return fmt.Errorf("Err: %s, Quitting", err)
					}
					buffer = make([]OplogDoc, 0, BUFFER_SIZE)
				}
				buffer = append(buffer, currentOp)
				logger.Finest("buffering op %s - (%d/%d) ", currentOp.String(), len(buffer), cap(buffer))
			} else {
				logger.Finest("skipping op %s - (%d/%d) ", currentOp.String(), len(buffer), cap(buffer))
			}
		}

		if iter.Timeout() {
			// take a break, ping the destination mongo to make sure all is well
			// write the current buffer to the dest, and ping the destination mongo
			if err := l.writeBuffer(buffer); err != nil {
				return fmt.Errorf("Err: %s, Quitting", err)
			}
			buffer = buffer[:0]

			// check to see if we're expired, i.e. if now is > our specified end point
			now := NewTimestamp("now")
			if now >= l.to {
				iter.Close()
				break outer
			}

			if err := l.target.KeepAlive(); err != nil {
				logger.Critical("Cannot reconnect to destination, quitting, %v", err)
				os.Exit(1)
			}
			continue
		}
		logger.Debug("Tailing cursor has become invalid, requerying oplog")
		iter = sourceOplog.Find(bson.M{"ts": bson.M{"$gt": lastTimestamp}}).Sort("$natural").Tail(1 * time.Second)
	}
	return nil
}

// some commands we don't want to replicate over, syncing over a copydb, dropDatabase or replSetINitiate seems like a bad idea
var blacklistedCommands = []string{"dropDatabase", "copydb", "replSetFreeze", "replSetInitiate", "replSetMaintenance", "replSetReconfig", "replSetStepDown", "replSetSyncFrom", "resync"}

func isBlacklistedCommand(doc OplogDoc) bool {
	var ok bool
	for _, v := range blacklistedCommands {
		_, ok = doc.O[v]
		if ok {
			return true
		}
	}
	return false
}
