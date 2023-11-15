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

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var (
	logs          *log.Logger = log.New(os.Stdout, "", 0)
	routineNumber int         = 5
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
		// Logger: logger.Default.LogMode(logger.Info),
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
	for scanner.Scan() {
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
		}

		if err != nil {
			logs.Printf("execute command failed: %v. exiting at %s line %v", err, filePath, lineCount)
			return
		}
	}

	logs.Printf("exits normally")
}
