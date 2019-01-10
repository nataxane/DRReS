package core

import (
	"fmt"
)

type Record string

type Table map[string]Record

type Storage struct {
	tables map[string](Table)
	logger DBLogger
}

func InitStorage() (storage Storage) {
	logger := initLogger("DRReS.log")

	storage = Storage{
		map[string]Table{"default": {}},
		logger}
	return
}

func (s Storage) Read(tableName string, key string) (value Record, err error) {
	table := s.tables[tableName]
	value, ok := table[key]
	if ok == false {
		return "", fmt.Errorf("key %v is not in the table", key)
	} else {
		return value, nil
	}
}

func (s Storage) Insert(tableName string, key string, value Record) (err error) {
	table := s.tables[tableName]
	_, ok := table[key]
	if ok == true {
		return fmt.Errorf("key %v is already in the table", key)
	} else {
		logEntry := fmt.Sprintf("put\t%s\t%s\n", key, value)
		s.logger.writeToDisk(logEntry)
		table[key] = value
		return
	}
}

func (s Storage) Update(tableName string, key string, value Record) (err error) {
	table := s.tables[tableName]
	_, ok := table[key]
	if ok == false {
		return fmt.Errorf("key %v is not in the table", key)
	} else {
		logEntry := fmt.Sprintf("put\t%s\t%s\n", key, value)
		s.logger.writeToDisk(logEntry)
		table[key] = value
		return
	}
}

func (s Storage) Delete(tableName string, key string) (err error) {
	table := s.tables[tableName]
	_, ok := table[key]
	if ok == false {
		return fmt.Errorf("key %v is not in the table", key)
	} else {
		logEntry := fmt.Sprintf("delete\t%s\n", key)
		s.logger.writeToDisk(logEntry)
		delete(table, key)
		return nil
	}
}

