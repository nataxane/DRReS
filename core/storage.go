package core

import (
	"fmt"
	"sync"
)

type Record string

type Storage struct {
	table *sync.Map
	logger DBLogger
}

func InitStorage() (storage Storage) {
	dbLogger := initLogger()
	table := &sync.Map{}

	storage = Storage{
		table,
		dbLogger,
	}

	storage.Recover()
	return
}

func (s Storage) Read(key string) (string, error) {
	value, recordOk := s.table.Load(key)
	if recordOk == false {
		return "", fmt.Errorf("key %v is not in the table", key)
	} else {
		return fmt.Sprint(value), nil
	}
}

func (s Storage) Insert(key string, value Record) error {
	_, recordOk := s.table.Load(key)

	if recordOk == true {
		return fmt.Errorf("key %v is already in the table", key)
	} else {
		logEntry := fmt.Sprintf("insert %s %s\n", key, value)
		s.logger.writeToDisk(logEntry)
		s.table.Store(key, value)
		return nil
	}
}

func (s Storage) Update(key string, value Record) error {
	_, recordOk := s.table.Load(key)

	if recordOk == false {
		return fmt.Errorf("key %v is not in the table", key)
	} else {
		logEntry := fmt.Sprintf("update %s %s\n", key, value)
		s.logger.writeToDisk(logEntry)
		s.table.Store(key, value)
		return nil
	}
}

func (s Storage) Delete(key string) error {
	_, recordOk := s.table.Load(key)

	if recordOk == false {
		return fmt.Errorf("key %v is not in the table", key)
	} else {
		logEntry := fmt.Sprintf("delete %s\n", key)
		s.logger.writeToDisk(logEntry)
		s.table.Delete(key)
		return nil
	}
}


