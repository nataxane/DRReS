package core

import (
	"fmt"
	"strings"
)

func ProcessQuery(query string, storage Storage) (result []byte) {
	op, key, value := parseQuery(query)

	table := storage.tables["default"]

	switch op {
	case "":
		result = []byte("Empty query")
	case "get":
		result = processGetQuery(key, table)
	case "put":
		result = processPutQuery(key, value, table)
	case "delete":
		result = processDeleteQuery(key, table)
	case "show":
		result = showTable(table)
	default:
		result = []byte("Unknown operation")
	}

	return
}

func parseQuery(query string) (op string, key string, value string) {
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

func processGetQuery(key string, table Table) (result []byte) {
	if key == "" {
		result = []byte("Empty key")
		return
	}

	value, err := table.Get(key)
	if err == nil {
		result = []byte(value)
	} else {
		result = []byte(err.Error())
	}

	return
}

func processPutQuery(key string, value string, table Table) (result []byte) {
	if key == "" {
		result = []byte("Empty key")
		return
	}

	err := table.Put(key, Record(value))
	if err == nil {
		result = []byte("ok")
	} else {
		result = []byte(err.Error())
	}

	return
}

func processDeleteQuery(key string, table Table) (result []byte) {
	if key == "" {
		result = []byte("Empty key")
		return
	}

	err := table.Delete(key)
	if err == nil {
		result = []byte("ok")
	} else {
		result = []byte(err.Error())
	}

	return
}

func showTable(table Table) (result []byte) {
	var records []string

	for key, value := range table {
		records = append(records, fmt.Sprintf("%s\t%s", key, value))
	}

	result = []byte(strings.Join(records, "\n"))
	return
}
