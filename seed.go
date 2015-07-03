package main

import (
	"code.google.com/p/log4go"
	"errors"
	"flag"
	"fmt"
	"gopkg.in/mgo.v2"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"
)

var (
	logdebug  = flag.Bool("v", false, "debug")
	logfinest = flag.Bool("vv", false, "super debug")
	logStats  = flag.Bool("stats", false, "show period mongostats")
)

var (
	source_uri = flag.String("s", "", "set the source mongo uri")
	dest_uri   = flag.String("d", "", "set the destination mongo uri")
)

var (
	// you won't need these for normal use
	from = flag.String("from", "now", "begin processing the oplog at this timestamp.  accepts timestemps in the form -from=+1 (now + 1 second), -from=now, -from=12345,123 (seconds, iterations)")
	to   = flag.String("to", "+86400", "begin processing the oplog at this timestamp.  accepts timestemps in the form -to=+1 (now + 1 second), -to=now, -to=12345,123 (seconds, iterations)")
)

var (
	initial_sync      = flag.Bool("i", false, "perform an initial sync")
	forceTableScan    = flag.Bool("forceTableScan", false, "don't sort by ids in initial sync. (sorting by _id can sometimes miss documents if _id is a non ObjectId())")
	forceIndexBuild   = flag.String("forceindex", "", "force Index builds on to either forground or background.  -forceindex foreground OR -forceindex background")
	replay_oplog      = flag.Bool("o", false, "replay the oplog from -from to -to")
	oplog_name        = flag.String("oplog", "oplog.rs", "the name of the oplog to use")
	ignore_errors     = flag.Bool("f", true, "force oplog sync, even if counts don't quite match (hope they match after oplog is sync'd)")
	allDbs            = flag.Bool("allDbs", false, "copy all the databases from the source to the destination")
	ignoreSslError    = flag.Bool("ignoreSslError", false, "ignore validation of SSL certificate")
	connectionTimeout = flag.Int("connectionTimeout", 60, "connection timeout in seconds")
)

var logger log4go.Logger

const (
	BUFFER_SIZE     = 2500
	GOPOOL_SIZE     = 16
	MAX_BUFFER_SIZE = 4e7
)

func main() {
	flag.Parse()
	logger = initLogger()

	runtime.GOMAXPROCS(runtime.NumCPU())

	if *source_uri == "" || *dest_uri == "" {
		Quit(1, errors.New("Provide both source and destination URIs"))
	}

	switch *forceIndexBuild {
	case "bg", "background":
		*forceIndexBuild = "bg"
	case "fg", "foreground":
		*forceIndexBuild = "fg"
	case "immediate":
		*forceIndexBuild = "im"
	case "":
		*forceIndexBuild = ""
	default:
		Quit(1, errors.New("Please provide a valid forceindex, either foreground or background, or immediate"))
	}

	if *source_uri == *dest_uri {
		Quit(1, errors.New("Source and destination can't be the same"))
	}

	// establish connections to source and destination mongos
	srcURI, srcDB, err := adminifyURI(*source_uri)
	if err != nil {
		Quit(1, err)
	}

	srcTarget := NewMongoTarget(srcURI, srcDB)
	err = srcTarget.Dial()
	source := srcTarget.dst
	if err != nil {
		logger.Critical("Cannot dial %s\n, %v", srcURI.String(), err)
		Quit(1, err)
	}
	source.SetMode(mgo.Monotonic, false)

	dstURI, dstDB, err := adminifyURI(*dest_uri)
	if err != nil {
		Quit(1, err)
	}

	target := NewTarget(dstURI, dstDB)
	err = target.Dial()
	if err != nil {
		Quit(1, fmt.Errorf("Cannot dial %s\n, %v", *dest_uri, err))
	}

	from := NewTimestamp(*from)
	to := NewTimestamp(*to)

	if *logStats {
		// periodically dump some mongo stats to stdout
		mgo.SetStats(true)
		statsChan := time.Tick(2 * time.Second)
		go func(c <-chan time.Time) {
			for _ = range c {
				s := mgo.GetStats()
				logger.Info("Sockets in use: %d, sentOps: %d, receivedOps: %d, receivedDocs: %d", s.SocketsInUse, s.SentOps, s.ReceivedOps, s.ReceivedDocs)
			}
		}(statsChan)
	}

	if *replay_oplog {
		//from, err = CurrentOplogTimestamp(source)
		//if err != nil {
		//	Quit(1, errors.New("unable to get most recent oplog timestamp"))
		//}
		logger.Info("Oplog at: %s", from)
	}
	if *initial_sync {
		logger.Info("copying data from %s to %s.", *source_uri, *dest_uri)
		// copying all the databases
		if *allDbs {
			databaseNames, err := source.DatabaseNames()
			if err != nil {
				Quit(1, err)
			}
			for _, d := range databaseNames {
				if isSpecialDatabase(d) {
					continue
				}
				logger.Info("Copying db " + d)
				target.DB(d)

				if err := target.Sync(source, srcURI, d); err != nil {
					Quit(1, err)
				}
			}

		} else {
			// copying one database
			if err := target.Sync(source, srcURI, srcDB); err != nil {
				Quit(1, err)
			}
		}
		logger.Info("initial sync completed")
	}

	if *replay_oplog {
		player, err := newLogReplayer(source, target, from, to, srcDB, dstDB)
		if err != nil {
			logger.Critical("Could not initialize logReplayer: %s", err)
			Quit(1, err)
		}

		err = player.playLog()
		if err != nil {
			logger.Error(err)
		}
	}
	Quit(0, nil)
}

func initLogger() log4go.Logger {
	loglevel := log4go.INFO
	if *logdebug {
		loglevel = log4go.DEBUG
	}
	if *logfinest {
		loglevel = log4go.FINEST
	}

	return log4go.NewDefaultLogger(loglevel)
}

func Quit(code int, err error) {
	if err != nil {
		logger.Critical("Failed: " + err.Error())
	}
	time.Sleep(100 * time.Millisecond)
	logger.Close()
	os.Exit(code)
}

func adminifyURI(s string) (*url.URL, string, error) {
	uri, err := url.Parse(s)
	if err != nil {
		return uri, "", err
	}

	db := uri.Path
	if len(db) < 2 {
		return uri, "", fmt.Errorf("invalid database name")
	}
	if db[0] == '/' {
		db = db[1:]
	}

	uri.Path = "/admin"

	logger.Debug("Connecting to URL: %v", uri)

	return uri, db, nil
}

// is the database a special database
var specialDatabases = []string{"admin", "local", "test", "config"}

func isSpecialDatabase(item string) bool {
	item = strings.ToLower(item)
	for _, v := range specialDatabases {
		if v == item {
			return true
		}
	}
	return false
}
