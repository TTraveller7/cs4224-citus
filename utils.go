package main

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
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

func FormatInt64Set(arr []int64) string {
	sb := strings.Builder{}
	sb.WriteString("(")
	for i, num := range arr {
		sb.WriteString(fmt.Sprintf("%v", num))
		if i != len(arr)-1 {
			sb.WriteString(",")
		}
	}
	sb.WriteString(")")
	return sb.String()
}

func Retry(query func() error) (err error) {
	for i := 0; i < RetryTimes; i++ {
		err = query()
		if err == nil {
			return nil
		}
		time.Sleep(GetBackoffTime())
	}
	return fmt.Errorf("exceeds retry limit: %v", err)
}

func GetBackoffTime() time.Duration {
	dF := rand.Float64()*float64(BackOffTimeMax-BackoffTimeMin) + BackoffTimeMin
	d := int(math.Round(dF))
	duration, _ := time.ParseDuration(fmt.Sprintf("%vms", d))
	return duration
}
