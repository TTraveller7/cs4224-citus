package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/montanaflynn/stats"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var (
	logs          *log.Logger = log.New(os.Stdout, "", 0)
	routineNumber int         = 1
)

func main() {
	args := os.Args

	if len(args) < 4+routineNumber {
		logs.Printf("not enough arguments: args=%+v", args)
		return
	}
	logs.Printf("main starting. Arguments: %+v, NumOfCPU:%v", args, runtime.NumCPU())

	taskIndexStr := os.Args[2]
	taskIndex, err := strconv.ParseInt(taskIndexStr, 10, 64)
	if err != nil {
		logs.Printf("convert task index failed: %v", err)
		return
	}
	ip := os.Args[3]
	filePaths := os.Args[4:]

	// TODO
	dsn := fmt.Sprintf("host=%s user=cs4224s password= dbname=project port=5115 sslmode=disable", ip)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		logs.Printf("open postgres client failed: %v", err)
		return
	}

	var wg sync.WaitGroup
	for i := 0; i < routineNumber; i++ {
		wg.Add(1)

		routineIndex := i + int(taskIndex)

		logs.Printf("starting routine #%v", routineIndex)
		go func() {
			defer wg.Done()
			execute(routineIndex, db, filePaths[routineIndex])
		}()
	}

	wg.Wait()
	logs.Printf("all routines joined. main exits normally")
}

func execute(routineIndex int, db *gorm.DB, filePath string) {
	logs := log.New(os.Stdout, fmt.Sprintf("[routine #%v] ", routineIndex), 0)
	logs.Printf("starts. filePath=%s", filePath)

	defer func() {
		if err := recover(); err != nil {
			logs.Printf("recover from panic. Error: \n%v", err)
		} else {
			logs.Printf("exits normally")
		}
	}()

	file, err := os.Open(filePath)
	if err != nil {
		logs.Printf("open file failed: %v", err)
		return
	}
	defer file.Close()

	lineCount := 0
	scanner := bufio.NewScanner(file)

	// metrics
	var counter int64 = 0
	latencies := make([]float64, 0)
	routineStart := time.Now()

	for scanner.Scan() {
		start := time.Now()

		lineCount++
		cmd := scanner.Text()
		words := strings.Split(cmd, ",")
		if len(words) == 0 {
			logs.Printf("cmd is empty at line %v", lineCount)
			return
		}

		var err error
		switch words[0] {
		case "N":
			err = NewOrder(logs, db, words, scanner, &lineCount)
		case "P":
			err = Payment(logs, db, words, scanner, &lineCount)
		case "D":
			err = Delivery(logs, db, words, scanner, &lineCount)
		case "O":
			err = OrderStatus(logs, db, words, scanner, &lineCount)
		case "S":
			err = StockLevel(logs, db, words, scanner, &lineCount)
		case "I":
			err = PopularItem(logs, db, words, scanner, &lineCount)
		case "T":
			err = TopBalance(logs, db, words, scanner, &lineCount)
		case "R":
			err = RelatedCustomer(logs, db, words, scanner, &lineCount)
		}

		if err != nil {
			logs.Printf("execute command failed: %v. exiting at %s line %v", err, filePath, lineCount)
			return
		}

		end := time.Now()
		latency := end.Sub(start)
		latencies = append(latencies, float64(latency.Milliseconds()))
		counter++
	}

	routineEnd := time.Now()
	totalLatency := routineEnd.Sub(routineStart).Milliseconds()
	throughPut := float64(counter) / float64(totalLatency)
	avgLatency, _ := stats.Mean(latencies)
	medianLatency, _ := stats.Median(latencies)
	nintyFivePercentile, _ := stats.Percentile(latencies, 95.0)
	nintyNinePercentile, _ := stats.Percentile(latencies, 99.0)

	metricsFile, err := os.OpenFile(fmt.Sprintf("/home/stuproj/cs4224s/%v_metrics.txt", routineIndex), os.O_RDWR|os.O_TRUNC, 0777)
	if err != nil {
		logs.Printf("open metrics file failed: %v", err)
		return
	}
	metricsFile.Write([]byte(fmt.Sprintf("%v %v %.2f %.2f %.2f %.2f %.2f", counter, totalLatency, throughPut, avgLatency, medianLatency, nintyFivePercentile, nintyNinePercentile)))
}
