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
	case "get":
		result = processGetQuery(storage,"default", key)
	case "put":
		result = processPutQuery(storage, "default", key, value)
	case "delete":
		result = processDeleteQuery(storage, "default", key)
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

func processGetQuery(storage Storage, tableName, key string) (result []byte) {
	if key == "" {
		result = []byte("Empty key")
		return
	}

	value, err := storage.Get(tableName, key)
	if err == nil {
		result = []byte(value)
	} else {
		result = []byte(err.Error())
	}

	return
}

func processPutQuery(storage Storage, tableName, key, value string) (result []byte) {
	if key == "" {
		result = []byte("Empty key")
		return
	}

	err := storage.Put(tableName, key, Record(value))
	if err == nil {
		result = []byte("ok")
	} else {
		result = []byte(err.Error())
	}

	return
}

func processDeleteQuery(storage Storage, tableName, key string) (result []byte) {
	if key == "" {
		result = []byte("Empty key")
		return
	}

	err := storage.Delete(tableName, key)
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
