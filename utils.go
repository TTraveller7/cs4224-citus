package main

import (
	"fmt"
	"strconv"
	"time"
)

func SafeParseInt(s string) int {
	res, _ := strconv.ParseInt(s, 10, 64)
	return int(res)
}

func Retry(query func() error) (err error) {
	for i := 0; i < RetryTimes; i++ {
		err = query()
		if err == nil {
			return nil
		}
		time.Sleep(BackoffTime)
	}
	return fmt.Errorf("exceeds retry limit: %v", err)
}
