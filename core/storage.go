package core

import (
	"fmt"
	"log"
	"os"
)

type Record string

type Table map[string]Record

type Storage struct {
	tables map[string](Table)
	logger *log.Logger
}

func InitStorage() (storage Storage) {
	f, err := os.OpenFile("DRReS.log", os.O_WRONLY | os.O_CREATE | os.O_APPEND, 0666)

	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}

	logger := log.New(f, "", log.LstdFlags)

	storage = Storage{
		map[string]Table{"default": {}},
		logger}
	return
}

func (s Storage) Get(tableName string, key string) (value Record, err error) {
	table := s.tables[tableName]
	value, ok := table[key]
	if ok == false {
		return "", fmt.Errorf("key %v is not in the table", key)
	} else {
		return value, nil
	}
}

func (s Storage) Put(tableName string, key string, value Record) (err error) {
	table := s.tables[tableName]
	_, ok := table[key]
	if ok == true {
		return fmt.Errorf("key %v is already in the table", key)
	} else {
		s.logger.Printf("put\t%s\t%s\n", key, value)
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
		s.logger.Printf("delete\t%s\n", key)
		delete(table, key)
		return nil
	}
}

