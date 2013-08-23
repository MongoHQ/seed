package main

import (
	"labix.org/v2/mgo"
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

func (b BlackHoleTarget) Apply(buf []OplogDoc) (ApplyOpResult, error) {
	logger.Debug("Blackholing %d ops", len(buf))
	return ApplyOpResult{Ok: 1, Applied: len(buf)}, nil
}

func (b BlackHoleTarget) Sync(src *mgo.Session, srcURI *url.URL, srcDB string) error {
	logger.Debug("Performing initial black hole sync, but nothing to do")
	return nil
}
