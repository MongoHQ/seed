package main

import (
	"fmt"
	"labix.org/v2/mgo/bson"
	"math"
	"strconv"
	"strings"
	"time"
)

type timestamp bson.MongoTimestamp

func NewTimestamp(ts string) timestamp {
	if len(ts) == 0 {
		return 0
	}

	switch {
	case ts == "inf":
		return timestamp(math.MaxInt64)

	case ts[0] == '+':
		offset, ok := strconv.Atoi(ts[1:])
		if ok != nil {
			offset = 0
		}
		return timestamp(int64(time.Now().Unix()+int64(offset)) << 32)

	case ts[0] == '-':
		offset, ok := strconv.Atoi(ts[1:])
		if ok != nil {
			offset = 0
		}
		return timestamp(int64(time.Now().Unix()-int64(offset)) << 32)

	case strings.ToLower(ts) == "now":
		return timestamp(time.Now().Unix() << 32)

	default:
		x := strings.Split(ts, ",")
		if len(x) == 0 {
			return 0
		}

		seconds, ok := strconv.Atoi(x[0])
		if ok != nil {
			return 0
		}

		if len(x) < 2 {
			return toTimestamp(seconds, 0)
		}
		iteration, ok := strconv.Atoi(x[1])
		if ok != nil {
			iteration = 0
		}
		return toTimestamp(seconds, iteration)
	}
}

func (ts timestamp) String() string {
	seconds, iteration := int32(ts>>32), int32(ts)
	return fmt.Sprintf("Timestamp(%d, %d)", seconds, iteration)
}

func toTimestamp(s, i int) timestamp {
	return timestamp(int64(s)<<32 + int64(i))
}
