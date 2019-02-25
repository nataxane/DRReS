package core

import (
	"fmt"
	"log"
	"os"
	"time"
)

type DBLogger struct {
	logger *log.Logger
	logFile *os.File
}

func initLogger() (logger DBLogger) {
	f, err := os.OpenFile(logFileName, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)

	if err != nil {
		log.Fatalf("error opening log file: %v", err)
	}

	logger.logFile = f
	logger.logger = log.New(f, "", 0)

	return
}

func (logger DBLogger) writeToDisk(logEntry string) int64  {
	currentPos, _ := logger.logFile.Seek(0, 1)
	logger.logger.Print(fmt.Sprintf("%d %s", time.Now().UnixNano()/1000, logEntry))
	err := logger.logFile.Sync()

	if err != nil {
		log.Fatalf("error flushing log to disk: %v", err)
	}

	return currentPos
}

