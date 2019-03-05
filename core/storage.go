package core

import (
	"fmt"
	"github.com/robfig/cron"
	"os"
	"sort"
	"sync"
)

type Record string


type Storage struct {
	table *sync.Map
	logger DBLogger
	checkpointScheduler *cron.Cron
	statsScheduler *cron.Cron
	Stats *RWStats
}

func InitStorage() (storage Storage) {
	dbLogger := initLogger()

	storage = Storage{
		table: &sync.Map{},
		logger: dbLogger,
		Stats:  &RWStats{},
	}

	storage.Recover()

	storage.RunStats()
	storage.RunCheckpointing()

	return
}

func (s *Storage) Stop() {
	s.checkpointScheduler.Stop()
	s.statsScheduler.Stop()
	s.Stats.DumpToDisk()
	//s.DumpToDisk(backupFileName)
}

func (s *Storage) Read(key string) (string, error) {
	s.Stats.readOp += 1

	value, recordOk := s.table.Load(key)
	if recordOk == false {
		return "", fmt.Errorf("key %v is not in the table", key)
	} else {
		return fmt.Sprint(value), nil
	}
}

func (s *Storage) Insert(key string, value Record) error {
	s.Stats.readOp += 1
	_, recordOk := s.table.Load(key)

	if recordOk == true {
		return fmt.Errorf("key %v is already in the table", key)
	} else {
		logEntry := fmt.Sprintf("insert %s %s\n", key, value)
		s.logger.writeToDisk(logEntry)

		s.table.Store(key, value)
		s.Stats.writeOp += 1

		return nil
	}
}

func (s *Storage) Update(key string, value Record) error {
	s.Stats.readOp += 1
	_, recordOk := s.table.Load(key)

	if recordOk == false {
		return fmt.Errorf("key %v is not in the table", key)
	} else {
		logEntry := fmt.Sprintf("update %s %s\n", key, value)
		s.logger.writeToDisk(logEntry)

		s.table.Store(key, value)
		s.Stats.writeOp += 1

		return nil
	}
}

func (s *Storage) Delete(key string) error {
	s.Stats.readOp += 1
	_, recordOk := s.table.Load(key)

	if recordOk == false {
		return fmt.Errorf("key %v is not in the table", key)
	} else {
		logEntry := fmt.Sprintf("delete %s\n", key)
		s.logger.writeToDisk(logEntry)

		s.table.Delete(key)
		s.Stats.writeOp += 1

		return nil
	}
}

func (s *Storage) DumpToDisk(fileName string) {
	backupFile, _ := os.OpenFile(fileName, os.O_WRONLY | os.O_CREATE, 0666)
	defer backupFile.Close()

	var recs []string

	mapToArray := func(key, value interface{}) bool {
		record := fmt.Sprintf("%s\t%s\n", key, value)
		recs = append(recs, record)
		return true
	}

	s.table.Range(mapToArray)
	sort.Strings(recs)

	for _, str := range recs {
		backupFile.Write([]byte(str))
	}
}
