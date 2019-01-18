package core

import (
	"strings"
)

func ProcessQuery(query string, storage Storage) (result []byte) {
	op, key, value := parseQuery(query)

	switch op {
	case "":
		result = []byte("Empty query")
	case "read":
		result = processReadQuery(storage, key)
	case "insert":
		result = processWriteQuery(storage, op, key, value)
	case "update":
		result = processWriteQuery(storage, op, key, value)
	case "delete":
		result = processWriteQuery(storage, op, key, value)
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

func processReadQuery(storage Storage, key string) (result []byte) {
	if key == "" {
		result = []byte("Empty key")
		return
	}

	value, err := storage.Read(key)
	if err == nil {
		result = []byte(value)
	} else {
		result = []byte(err.Error())
	}

	return
}

func processWriteQuery(storage Storage, op, key, value string) (result []byte){
	if key == "" {
		result = []byte("Empty key")
		return
	}

	var err error

	switch op {
	case "insert":
		err = storage.Insert(key, Record(value))
	case "update":
		err = storage.Update(key, Record(value))
	case "delete":
		err = storage.Delete(key)
	}

	if err == nil {
		result = []byte("ok")
	} else {
		result = []byte(err.Error())
	}

	return
}

