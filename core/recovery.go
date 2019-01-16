package core

import (
	"bufio"
	"log"
	"strings"
)

func restoreCheckpoint(s Storage) {

}

func redoLog(s Storage) {
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
			log.Printf("Skip invalid query in the log: %v", query)
		}
	}
}

func (s Storage) Recover() {
	restoreCheckpoint(s)
	redoLog(s)

	log.Printf("Successfully recovered %d rows", len(s.tables["default"]))
}
