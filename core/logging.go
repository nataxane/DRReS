package core

import (
	"log"
	"os"
)

type DBLogger struct {
	logger *log.Logger
	logFile *os.File
}

func initLogger(logfileName string) (logger DBLogger) {
	f, err := os.OpenFile(logfileName, os.O_WRONLY | os.O_CREATE | os.O_APPEND, 0666)

	if err != nil {
		log.Fatalf("error opening log file: %v", err)
	}

	logger.logFile = f
	logger.logger = log.New(f, "", log.LstdFlags)

	return
}

func (logger DBLogger) writeToDisk(logEntry string) {
	logger.logger.Print(logEntry)
	err := logger.logFile.Sync()

	if err != nil {
		log.Fatalf("error flushing log to disk: %v", err)
	}
}

