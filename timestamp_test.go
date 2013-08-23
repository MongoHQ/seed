package main

import (
	"testing"
	"time"
)

func TestNewTimestamp(t *testing.T) {

	now := time.Now().Unix()

	data := []struct {
		in string
		s  int
		i  int
	}{
		{"now", int(now), 0},
		{"+0", int(now), 0},
		{"+1", int(now) + 1, 0},
		{"", 0, 0},
		{"12345,", 12345, 0},
		{"12345,", 12345, 0},
		{"12345,0", 12345, 0},
		{"12345,1", 12345, 1},
	}

	for _, v := range data {
		if toTimestamp(v.s, v.i) != NewTimestamp(v.in) {
			t.Errorf("expected %s, got %s (%d)", toTimestamp(v.s, v.i), NewTimestamp(v.in), NewTimestamp(v.in))
		}
	}

}
