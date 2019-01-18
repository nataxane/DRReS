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

		switch {
		case op == "insert" || op =="update":
			s.table.Store(key, Record(value))
		case op == "delete":
			s.table.Delete(key)
		default:
			log.Printf("Skip invalid query in the log: %v", query)
		}
	}
}

func (s Storage) Recover() {
	restoreCheckpoint(s)
	redoLog(s)
}
