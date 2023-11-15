package main

import (
	"fmt"
	"strconv"
	"time"
)

var ErrNoRowsAffected = fmt.Errorf("affected 0 rows")

func SafeParseInt(s string) int {
	res, _ := strconv.ParseInt(s, 10, 64)
	return int(res)
}

func SafeParseInt64(s string) int64 {
	res, _ := strconv.ParseInt(s, 10, 64)
	return res
}

func SafeParseFloat64(s string) float64 {
	res, _ := strconv.ParseFloat(s, 64)
	return res
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
