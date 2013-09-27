package main

import (
	"labix.org/v2/mgo"
	"net/url"
	"strings"
)

type Target interface {
	Sync(src *mgo.Session, srcURI *url.URL, srcDB string) error
	ApplyOne(op OplogDoc) error
	KeepAlive() error
	Dial() error
	DB(string)
	Close()
}

func NewTarget(uri *url.URL, dstDB string) Target {
	switch strings.ToLower(uri.Scheme) {
	case "mongodb":
		return NewMongoTarget(uri, dstDB)
	case "blackhole":
		return BlackHoleTarget(uri.String())
	default:
		return BlackHoleTarget(uri.String())
	}
}
