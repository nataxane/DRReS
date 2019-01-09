package core

import (
	"fmt"
)

type Record string

type Table map[string]Record

type Storage struct {
	tables map[string](Table)
}

func InitStorage() (storage Storage) {
	storage = Storage{map[string]Table{"default": {}}}
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
		delete(table, key)
		return nil
	}
}

