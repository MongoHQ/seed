package main

import (
	"gopkg.in/mgo.v2"
	"net/url"
)

type BlackHoleTarget string

func (b BlackHoleTarget) Dial() error {
	logger.Debug("dialing %s", b)
	return nil
}

func (b BlackHoleTarget) Close() {
	logger.Debug("closing %s", b)
}

func (b BlackHoleTarget) DB(db string) {
}

func (b BlackHoleTarget) KeepAlive() error {
	logger.Debug("sending %s a keepalive", b)
	return nil
}

func (b BlackHoleTarget) ApplyOne(buf OplogDoc) error {
	logger.Debug("Blackholing ops")
	return nil
}

func (b BlackHoleTarget) Sync(src *mgo.Session, srcURI *url.URL, srcDB string) error {
	logger.Debug("Performing initial black hole sync, but nothing to do")
	return nil
}
