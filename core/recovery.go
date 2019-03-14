package core

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)


func getCheckpointList() ([]string, error) {
	allCheckpoints, err := ioutil.ReadFile(lastCheckpointFileName)
	checkpointList := strings.Split(string(allCheckpoints), "\n")

	if err != nil {
		return nil, err
	}

	return checkpointList[:len(checkpointList) - 1], nil
}

func restoreCheckpoint(s Storage, snapshotPath string) error {
	snapshotFile, err := os.OpenFile(snapshotPath, os.O_RDONLY, 0666)
	if err != nil {
		log.Printf("Can not load snapshot %s: %v", snapshotPath, err)
		return err
	}

	defer snapshotFile.Close()

	log.Printf("Start loading snapshot %s", snapshotPath)

	buf := make([]byte, recordSize)
	reader := bufio.NewReader(snapshotFile)
	recCount := 0

OUTLOOP:
	for {
		_, err := reader.Read(buf)

		switch err {
		case io.EOF:
			break OUTLOOP
		case nil:
			record := string(bytes.Trim(buf, "\x00"))
			kv := strings.Split(record, "\t")
			s.table.Store(kv[0], kv[1])
			recCount += 1
		default:
			log.Printf("Can not load snapshot: %v", err)
			return err
		}
	}

	log.Printf("Snapshot successfully loaded: %d records", recCount)
	return nil
}

func validateCheckpointLogEntry(logFile *os.File, logEntryId string, logPos int64) bool {
	logFile.Seek(logPos, 0)

	buf := make([]byte, recordSize)
	logFile.Read(buf)

	logEntryParsed := strings.Split(string(buf), " ")

	if logEntryParsed[0] != logEntryId {
		return false
	}
	return true
}


func parseLogEntry(logEntryBuf []byte) (int, int, string) {
	logEntry := string(bytes.Trim(logEntryBuf, "\x00"))
	logEntryParsed := strings.SplitN(logEntry, " ", 3)

	logEntryId := logEntryParsed[0]
	logEntryIdParsed := strings.Split(logEntryId, "_")
	wrapId, _ := strconv.Atoi(logEntryIdParsed[0])
	LSN, _ :=  strconv.Atoi(logEntryIdParsed[1])

	query := logEntryParsed[2]

	return wrapId, LSN, query
}


func applyLogEntry(s Storage, query string) {
	query = strings.TrimSuffix(query, "\n")

	op, key, value := parseQuery(query)

	switch {
	case op == "insert" || op == "update":
		s.table.Store(key, Record(value))
	case op == "delete":
		s.table.Delete(key)
	case op == "begin_checkpoint" || op == "end_checkpoint":
		break
	default:
		log.Printf("Skip invalid query in the log: %v", query)
	}
}

func redoLog(checkpointLogEntryPos int64, checkpointLogEntryId string, s Storage) (int64, int, int) {
	checkpointLogEntryOk := validateCheckpointLogEntry(s.logger.logFile, checkpointLogEntryId, checkpointLogEntryPos)

	if checkpointLogEntryOk == false {
		log.Println("Reloaded snapshot is not in the log. Skip log restoring.")
		return 0, 0, 0
	}

	checkpointLogEntryIdParsed := strings.Split(checkpointLogEntryId, "_")
	currentWrapId, _ := strconv.Atoi(checkpointLogEntryIdParsed[0])
	currentLSN, _ := strconv.Atoi(checkpointLogEntryIdParsed[1])

	offset := checkpointLogEntryPos + logEntrySize

	reader := bufio.NewReader(s.logger.logFile)
	buf := make([]byte, logEntrySize)

	newWrap := false

OUTLOOP:
	for {
		_, err := reader.Read(buf)

		switch err {
		case io.EOF:
			s.logger.logFile.Seek(0, 0)
			newWrap = true
		case nil:
			logEntryWrapId, logEntryLSN, query := parseLogEntry(buf)

			switch {
			case logEntryWrapId == currentWrapId && newWrap == false:
				currentPos, _ := s.logger.logFile.Seek(0, 1)
				offset = currentPos + logEntrySize
				currentLSN = logEntryLSN

				applyLogEntry(s, query)
			case logEntryWrapId > currentWrapId && newWrap == true:
				newWrap = false
				offset = logEntrySize
				currentLSN = logEntryLSN
				currentWrapId += 1

				applyLogEntry(s, query)
			default:
				break OUTLOOP
			}

		default:
			log.Printf("Can not redo log entries: %v", err)
		}
	}

	return offset, currentWrapId, currentLSN
}

func (s *Storage) Recover() {
	log.Print("Recovery started")

	start := time.Now().UnixNano()

	checkpoints, err := getCheckpointList()

	if err != nil {
		log.Printf("Recovery failed: %v", err)
		return
	}

	var (
		checkpointInfo []string
		snapshotRestoreError error
	)

	for i := len(checkpoints) - 1; i >= 0; i-- {
		checkpointInfo = strings.Split(checkpoints[i], "\t")

		snapshotRestoreError = restoreCheckpoint(*s, checkpointInfo[1])
		if snapshotRestoreError == nil {
			break
		}
	}

	if snapshotRestoreError != nil {
		log.Printf("Recovery failed: %v", snapshotRestoreError)
	}

	checkpointLogEntryPos, _ := strconv.ParseInt(checkpointInfo[2], 10, 64)

	logStart, wrapId, LSN := redoLog(checkpointLogEntryPos, checkpointInfo[0], *s)

	s.logger.offset = logStart
	s.logger.LSN = LSN + 1
	s.logger.wrapId = wrapId

	end := time.Now().UnixNano()

	log.Printf("Recovery finished: %.2f ms", float64(end - start)/ 1000 / 1000)
}
