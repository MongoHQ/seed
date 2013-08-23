package main

import (
	"fmt"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"strings"
)

type OpKind uint8

const (
	NOOP OpKind = iota
	INSERT
	DELETE
	UPDATE
	COMMAND
)

type ApplyOpResult struct {
	Applied int    "applied"
	Results []bool "results"
	Ok      int    "ok"
}

type OplogDoc struct {
	Ts bson.MongoTimestamp "ts"
	H  int64               "h"
	V  int                 "v"
	Op string              "op"
	Ns string              "ns"
	O  bson.M              "o"
	O2 bson.M              "o2"
}

func (o *OplogDoc) String() string {
	var kind string
	switch o.Kind() {
	case NOOP:
		kind = "noop"
	case INSERT:
		kind = "insert"
	case DELETE:
		kind = "delete"
	case UPDATE:
		kind = "update"
	case COMMAND:
		kind = "command"
	default:
		kind = fmt.Sprintf("unknown (%s)", o.Op)
	}

	return fmt.Sprintf("%s %s in %s", timestamp(o.Ts), kind, o.Ns)
}

func (op *OplogDoc) Kind() OpKind {
	switch op.Op {
	case "n":
		return NOOP
	case "d":
		return DELETE
	case "i":
		return INSERT
	case "u":
		return UPDATE
	case "c":
		return COMMAND
	default:
		panic(fmt.Sprintf("unknown op type %s", op.Op))
	}
}

func (op *OplogDoc) Database() string {
	return strings.SplitN(op.Ns, ".", 2)[0]
}

func (op *OplogDoc) Collection() string {
	return strings.SplitN(op.Ns, ".", 2)[1]
}

// CurrentOplogTimestamp fetches the most recent oplog entry and returns the timestamp
func CurrentOplogTimestamp(sess *mgo.Session) (timestamp, error) {
	doc := OplogDoc{}
	err := sess.DB("local").C(*oplog_name).Find(bson.M{}).Sort("-$natural").One(&doc)
	if err != nil {
		return timestamp(doc.Ts), err
	}
	return timestamp(doc.Ts), nil
}
