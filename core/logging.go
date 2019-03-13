package core

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type DBLogger struct {
	logFile *os.File
	LSN int
	wrapId int
	offset int64
	mx sync.Mutex
}

var logMaxSize = maxWriteQps * cpFreq * logEntrySize

func initLogger() (logger DBLogger) {
	f, err := os.OpenFile(logFileName, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)

	if err != nil {
		log.Fatalf("error opening log file: %v", err)
	}

	logger.logFile = f
	logger.offset = 0
	logger.wrapId = 0
	logger.LSN = 0

	return
}

func (logger *DBLogger) writeToDisk(query string) (int64, string)  {
	logger.mx.Lock()
	currentOffset := logger.offset

	logEntryId := fmt.Sprintf("%d_%d", logger.wrapId, logger.LSN)
	logEntry := fmt.Sprintf("%s %d %s\n", logEntryId, time.Now().UnixNano()/1000, query)

	logEntryBlock := make([]byte, logEntrySize)

	copy(logEntryBlock, []byte(logEntry))

	_, err := logger.logFile.WriteAt(logEntryBlock, currentOffset)
	if err != nil {
		log.Fatalf("Error while logging: %v", err)
	}

	err = logger.logFile.Sync()

	if err != nil {
		log.Fatalf("Error flushing log to disk: %v", err)
	}

	logger.LSN += 1
	logger.offset += logEntrySize


	if logger.offset >= int64(logMaxSize) {
		logger.offset = 0
		logger.LSN = 0
		logger.wrapId += 1
	}

	logger.mx.Unlock()

	return currentOffset, logEntryId
}

