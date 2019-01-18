package core

import (
	"fmt"
	"log"
	"sync"
)

const (
	logFileName = "DRReS.log"
	snapshotFileName = "db_snapshot"
	lastCheckpointFileName = "last_checkpoint"
)

type Record string

type Storage struct {
	tables map[string]sync.Map
	logger DBLogger
}

func InitStorage() (storage Storage) {
	dbLogger := initLogger()

	storage = Storage{
		map[string]sync.Map{},
		dbLogger,
	}

	log.Print("Recovery started")
	storage.Recover()
	log.Print("Recovery finished")
	return
}

func (s Storage) Read(tableName string, key string) (string, error) {
	table, tableOk := s.tables[tableName]

	if tableOk == false {
		return "", fmt.Errorf("table %v does not exist", tableName)
	}

	value, recordOk := table.Load(key)
	if recordOk == false {
		return "", fmt.Errorf("key %v is not in the table", key)
	} else {
		return fmt.Sprint(value), nil
	}
}

func (s Storage) Insert(tableName string, key string, value Record) error {
	table, tableOk := s.tables[tableName]

	if tableOk == false {
		table = sync.Map{}
		s.tables[tableName] = table
	}

	_, recordOk := table.Load(key)

	if recordOk == true {
		return fmt.Errorf("key %v is already in the table", key)
	} else {
		logEntry := fmt.Sprintf("insert %s %s\n", key, value)
		s.logger.writeToDisk(logEntry)
		table.Store(key, value)
		return nil
	}
}

func (s Storage) Update(tableName string, key string, value Record) error {
	table, tableOk := s.tables[tableName]

	if tableOk == false {
		return fmt.Errorf("table %v does not exist", tableName)
	}

	_, recordOk := table.Load(key)

	if recordOk == false {
		return fmt.Errorf("key %v is not in the table", key)
	} else {
		logEntry := fmt.Sprintf("update %s %s\n", key, value)
		s.logger.writeToDisk(logEntry)
		table.Store(key, value)
		return nil
	}
}

func (s Storage) Delete(tableName string, key string) error {
	table, tableOk := s.tables[tableName]

	if tableOk == false {
		return fmt.Errorf("table %v does not exist", tableName)
	}

	_, recordOk := table.Load(key)

	if recordOk == false {
		return fmt.Errorf("key %v is not in the table", key)
	} else {
		logEntry := fmt.Sprintf("delete %s\n", key)
		s.logger.writeToDisk(logEntry)
		table.Delete(key)
		return nil
	}
}


