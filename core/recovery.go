package core

import (
	"bufio"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)


func getLastCheckpoint() (string, int64){
	allCheckpoints, err := ioutil.ReadFile(lastCheckpointFileName)

	if err != nil {
		log.Printf("Can not load snapshot: %v", err)
		return "", 0
	}

	lines := strings.Split(string(allCheckpoints), "\n")
	lastCheckpoint := lines[len(lines) - 2]

	checkpointData := strings.Split(lastCheckpoint, "\t")
	snapshotPath := checkpointData[0]
	logPos, err := strconv.ParseInt(checkpointData[1], 10, 64)

	if err != nil {
		log.Printf("Can not load snapshot: %v", err)
		return "", 0
	}
	return snapshotPath, logPos
}


func restoreCheckpoint(s Storage) int64 {
	snapshotPath, logPos := getLastCheckpoint()

	if logPos == 0 {
		return 0
	}

	snapshotFile, err := os.OpenFile(snapshotPath, os.O_RDONLY, 0666)

	if err != nil {
		log.Printf("Can not load snapshot: %v", err)
		return 0
	} else {
		log.Printf("Start loading snapshot: %s", snapshotPath)
	}

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
			record := string(buf)
			kv := strings.Split(record, "\t")
			s.table.Store(kv[0], kv[1])
			recCount += 1
		default:
			log.Printf("Can not load snapshot: %v", err)
			return 0
		}
	}

	log.Printf("Snapshot successfully loaded: %d records", recCount)
	return logPos
}

/*
Right now it's not obvious why do we need timestamps
	insert/update already updated value – we will reupdate it later anyway
	delete already deleted value – sync.Map doesn't care
 */


func redoLog(startPos int64, s Storage) {
	s.logger.logFile.Seek(startPos, 0)  // go to the last begin_checkpoint entry in log

	logScanner := bufio.NewScanner(s.logger.logFile)

	for logScanner.Scan() {
		logEntry := logScanner.Text()
		query := strings.SplitN(logEntry, " ", 2)[1]

		op, key, value := parseQuery(query)

		switch {
		case op == "insert" || op == "update":
			s.table.Store(key, Record(value))
		case op == "delete":
			s.table.Delete(key)
		case op == "begin_checkpoint" || op == "end_checkpoint":
			continue
		default:
			log.Printf("Skip invalid query in the log: %v", query)
		}
	}
}

/*
Global ToDo:
	1. Validate recovery (do we really recovered to the last state of the DB?) +
	2. Make sure that we recover faster with checkpoints +
	3. Measure throughput with checkpoints 
 */

func (s Storage) Recover() {
	log.Print("Recovery started")

	start := time.Now().UnixNano()

	logStartPos := restoreCheckpoint(s)
	redoLog(logStartPos, s)

	//s.DumpToDisk(recoveredFileName)

	end := time.Now().UnixNano()

	log.Printf("Recovery finished: %.2f ms", float64(end - start)/ 1000 / 1000)
}
