package main

import (
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"net/url"
	"strings"
)

type TokuMXTarget struct {
	m *MongoTarget
}

// NewMongoTarget creates a new Mongodb Target
func NewTokuMXTarget(uri *url.URL, dstDB string) *TokuMXTarget {
	muri := uri
	muri.Scheme = "mongodb"

	return &TokuMXTarget{m: NewMongoTarget(muri, dstDB)}
}

func (t *TokuMXTarget) Dial() error {
	return t.m.Dial()
}

func (t *TokuMXTarget) DB(db string) {
	t.m.dstDB = db
}

func (t *TokuMXTarget) Close() {
	t.m.Close()
	return
}

func (t *TokuMXTarget) KeepAlive() error {
	return t.m.KeepAlive()
}

func (t *TokuMXTarget) Sync(src *mgo.Session, srcURI *url.URL, srcDB string) error {
	return t.m.Sync(src, srcURI, srcDB)
}

func (t *TokuMXTarget) Apply(ops []OplogDoc) (ApplyOpResult, error) {
	logger.Debug("want to apply %d ops to tokumx", len(ops))
	applied := 0
	for _, op := range ops {
		ok := t.ApplyOne(op)
		if ok {
			applied++
		}
	}
	return ApplyOpResult{Ok: 1, Applied: applied}, nil
}

//TODO don't want to err here on failed ops, l ikely just want to return a false
func (t TokuMXTarget) ApplyOne(op OplogDoc) bool {
	logger.Finest("%s Op: %+v", op.String(), op)
	switch op.Kind() {
	case INSERT:
		if op.Collection() == "system.indexes" {
			logger.Debug("Adding Index Op")
			ns, exists := op.O["ns"]
			if exists {
				op.O["ns"] = strings.Replace(ns.(string), t.m.srcDB, t.m.dstDB, 1)
			}
		}
		err := t.m.dst.DB(op.Database()).C(op.Collection()).Insert(op.O)
		return err == nil
	case UPDATE:
		err := t.m.dst.DB(op.Database()).C(op.Collection()).Update(op.O2, op.O)
		return err == nil
	case DELETE:
		err := t.m.dst.DB(op.Database()).C(op.Collection()).Remove(op.O)
		return err == nil
	case COMMAND:
		ns, exists := op.O["ns"]
		if exists {
			op.O["ns"] = strings.Replace(ns.(string), t.m.srcDB, t.m.dstDB, 1)
		}
		result := bson.M{}
		err := t.m.dst.DB(op.Database()).Run(op.O, &result)
		ok, exists := result["ok"]
		if (err == nil) && (exists && ok == 1) {
			return true
		} else {
			return false
		}
		return false
	}
	return false
}
