package core

import (
	"bufio"
	"fmt"
	"github.com/robfig/cron"
	"log"
	"os"
	"strconv"
	"time"
)

type Checkpointer struct {
	Scheduler *cron.Cron
	Quit chan bool
}

func RunCheckpointing(s Storage) *Checkpointer {
	cp := Checkpointer{
		Scheduler: cron.New(),
		Quit: make(chan bool),
	}

	err := cp.Scheduler.AddFunc(
		fmt.Sprintf("@every %ds", cpFreq),
		func() {cp.makeCheckpoint(s)})

	if err != nil {
		log.Panicf("Can not run checkpointing scheduler: %s", err)
	}

	cp.Scheduler.Start()

	return &cp
}

func (cp *Checkpointer) makeCheckpoint(storage Storage) {
	select {
	case <- cp.Quit:
		close(cp.Quit)
		return
	default:
		_makeCheckpoint(storage)
		return
	}
}

func _makeCheckpoint(storage Storage) {
	currentSnapshotFileName := fmt.Sprintf("%s/%s", snapshotDir, strconv.FormatInt(time.Now().Unix(), 10))
	snapshotFile, err := os.OpenFile(currentSnapshotFileName, os.O_WRONLY | os.O_CREATE, 0666)

	if err != nil {
		log.Printf("Can not make a checkpoint: %s\n", err)
		return
	}

	defer snapshotFile.Close()

	writer := bufio.NewWriter(snapshotFile)
	recCount := 0
	snapshotOk := true

	writeRecordToDisk := func(key, value interface{}) bool {
		record := []byte(fmt.Sprintf("%s\t%s", key, value))
		recordBlock := make([]byte, recordSize)
		copy(recordBlock, record)

		_, err = writer.Write(recordBlock)

		if err != nil {
			log.Printf("Can not make a checkpoint: %s\n", err)
			snapshotOk = false
			return false
		}

		recCount += 1

		return true
	}

	log.Println("Checkpoint: start")
	logPos := storage.logger.writeToDisk("begin_checkpoint")

	storage.table.Range(writeRecordToDisk)

	if snapshotOk != true {
		log.Printf("Can not make a checkpoint: %s\n", err)
		return
	}

	err = writer.Flush()
	if err != nil {
		log.Printf("Can not make a checkpoint: %s\n", err)
		return
	}

	saveCheckpoint(logPos, currentSnapshotFileName)

	storage.logger.writeToDisk("end_checkpoint")
	log.Printf("Checkpoint: end (%d records)", recCount)
}

func saveCheckpoint(logPos int64, fileName string) {
	lastCheckpointFile, _ := os.OpenFile(lastCheckpointFileName, os.O_WRONLY | os.O_APPEND | os.O_CREATE, 0666)
	defer lastCheckpointFile.Close()

	rec := fmt.Sprintf("%s\t%s\n", fileName, strconv.FormatInt(logPos, 10))

	_, err := lastCheckpointFile.Write([]byte(rec))

	if err != nil {
		log.Printf("Can not save a checkpoint: %s", err)
		return
	}
}