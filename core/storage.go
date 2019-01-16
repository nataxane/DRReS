package core

import (
	"bufio"
	"fmt"
	"log"
	"strings"
	"sync"
)

type Record string

type Table map[string]Record

type Storage struct {
	tables map[string](Table)
	logger DBLogger
	locker *sync.Mutex
}

func InitStorage() (storage Storage) {
	dbLogger := initLogger("DRReS.log")
	locker := &sync.Mutex{}

	storage = Storage{
		map[string]Table{},
		dbLogger,
		locker,
	}

	log.Print("Recovery started")
	storage.Recover()
	log.Print("Recovery finished")
	return
}

func (s Storage) Read(tableName string, key string) (value Record, err error) {
	table, tableOk := s.tables[tableName]

	if tableOk == false {
		return "", fmt.Errorf("table %v does not exist", tableName)
	}

	s.locker.Lock()
	value, recordOk := table[key]
	s.locker.Unlock()
	if recordOk == false {
		return "", fmt.Errorf("key %v is not in the table", key)
	} else {
		return value, nil
	}
}

func (s Storage) Insert(tableName string, key string, value Record) (err error) {
	table, tableOk := s.tables[tableName]

	if tableOk == false {
		table = Table{}
		s.tables[tableName] = table
	}

	s.locker.Lock()
	_, recordOk := table[key]
	s.locker.Unlock()
	if recordOk == true {
		return fmt.Errorf("key %v is already in the table", key)
	} else {
		logEntry := fmt.Sprintf("insert %s %s\n", key, value)
		s.logger.writeToDisk(logEntry)
		s.locker.Lock()
		table[key] = value
		s.locker.Unlock()
		return
	}
}

func (s Storage) Update(tableName string, key string, value Record) (err error) {
	table, tableOk := s.tables[tableName]

	if tableOk == false {
		return fmt.Errorf("table %v does not exist", tableName)
	}

	s.locker.Lock()
	_, recordOk := table[key]
	s.locker.Unlock()
	if recordOk == false {
		return fmt.Errorf("key %v is not in the table", key)
	} else {
		logEntry := fmt.Sprintf("update %s %s\n", key, value)
		s.logger.writeToDisk(logEntry)
		s.locker.Lock()
		table[key] = value
		s.locker.Unlock()
		return
	}
}

func (s Storage) Delete(tableName string, key string) (err error) {
	table, tableOk := s.tables[tableName]

	if tableOk == false {
		return fmt.Errorf("table %v does not exist", tableName)
	}

	s.locker.Lock()
	_, recordOk := table[key]
	s.locker.Unlock()
	if recordOk == false {
		return fmt.Errorf("key %v is not in the table", key)
	} else {
		logEntry := fmt.Sprintf("delete %s\n", key)
		s.logger.writeToDisk(logEntry)
		s.locker.Lock()
		delete(table, key)
		s.locker.Unlock()
		return nil
	}
}

func (s Storage) Recover() {
	logScanner := bufio.NewScanner(s.logger.logFile)

	for logScanner.Scan() {
		logEntry := logScanner.Text()
		query := &strings.SplitN(logEntry, " ", 3)[2]

		op, key, value := parseQuery(*query)
		tableName := "default"

		table, tableOk := s.tables[tableName]

		if tableOk == false {
			table = Table{}
			s.tables[tableName] = table
		}

		switch {
		case op == "insert" || op =="update":
			table[key] = Record(value)
		case op == "delete":
			delete(table, key)
		default:
			log.Fatalf("Recovery failed: invalid query in the log: %v", query)
		}
	}
	log.Printf("Successfully recovered %d rows", len(s.tables["default"]))
}

