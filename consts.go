package main

import "time"

const (
	RetryTimes  = 10
	BackoffTime = 500 * time.Millisecond
)
