package core

import (
	"fmt"
	"strings"
)

func ProcessQuery(query string, storage Storage) (result []byte) {
	op, key, value := parseQuery(query)

	switch op {
	case "":
		result = []byte("Empty query")
	case "read":
		result = processReadQuery(storage,"default", key)
	case "insert":
		result = processWriteQuery(storage, op, "default", key, value)
	case "update":
		result = processWriteQuery(storage, op, "default", key, value)
	case "delete":
		result = processWriteQuery(storage, op, "default", key, value)
	case "show":
		result = showTable(storage, "default")
	default:
		result = []byte("Unknown operation")
	}

	return
}

func parseQuery(query string) (op, key, value string) {
	splittedQuery := strings.SplitN(query, " ", 3)

	if len(splittedQuery) >= 1 {
		op = splittedQuery[0]
	}

	if len(splittedQuery) >= 2 {
		key = splittedQuery[1]
	}

	if len(splittedQuery) == 3 {
		value = splittedQuery[2]
	}
	return
}

func processReadQuery(storage Storage, tableName, key string) (result []byte) {
	if key == "" {
		result = []byte("Empty key")
		return
	}

	value, err := storage.Read(tableName, key)
	if err == nil {
		result = []byte(value)
	} else {
		result = []byte(err.Error())
	}

	return
}

func processWriteQuery(storage Storage, op, tableName, key, value string) (result []byte){
	if key == "" {
		result = []byte("Empty key")
		return
	}

	var err error

	switch op {
	case "insert":
		err = storage.Insert(tableName, key, Record(value))
	case "update":
		err = storage.Update(tableName, key, Record(value))
	case "delete":
		err = storage.Delete(tableName, key)
	}

	if err == nil {
		result = []byte("ok")
	} else {
		result = []byte(err.Error())
	}

	return
}

func showTable(storage Storage, tableName string) (result []byte) {
	var records []string

	for key, value := range storage.tables[tableName] {
		records = append(records, fmt.Sprintf("%s\t%s", key, value))
	}

	result = []byte(strings.Join(records, "\n"))
	return
}
