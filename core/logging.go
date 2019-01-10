package core

import (
	"log"
	"os"
)

func InitLogger(logfileName string) (logger *log.Logger){
	f, err := os.OpenFile(logfileName, os.O_WRONLY | os.O_CREATE | os.O_APPEND, 0666)

	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}

	logger = log.New(f, "", log.LstdFlags)
	return
}

func 