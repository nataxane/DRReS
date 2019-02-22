package core

import (
	"bufio"
	"fmt"
	"github.com/robfig/cron"
	"log"
	"math"
	"os"
	"strconv"
	"time"
)

func (s *Storage) RunCheckpointing() {
	s.checkpointScheduler = cron.New()

	err := s.checkpointScheduler.AddFunc(
		fmt.Sprintf("@every %ds", cpFreq),
		func() {makeCheckpoint(s)})
	if err != nil {
		log.Fatalf("Can not run checkpointing scheduler: %s", err)
	}

	s.checkpointScheduler.Start()
}

func makeCheckpoint(storage *Storage) {
	currentSnapshotFileName := fmt.Sprintf("%s/%s", snapshotDir, strconv.FormatInt(time.Now().Unix(), 10))
	snapshotFile, err := os.OpenFile(currentSnapshotFileName, os.O_WRONLY | os.O_CREATE, 0666)

	if err != nil {
		log.Printf("Can not make a checkpoint: %s\n", err)
		return
	}

	writer := bufio.NewWriter(snapshotFile)
	recCount := 0

	copyRecord := func(key, value interface{}) bool {
		record := []byte(fmt.Sprintf("%s\t%s", key, value))
		recordBlock := make([]byte, recordSize)
		copy(recordBlock, record)

		_, err = writer.Write(recordBlock)

		if err != nil {
			log.Panicln(err)
		}

		recCount += 1

		return true
	}

	logPos := storage.logger.writeToDisk("begin_checkpoint")
	log.Println("Checkpoint: start")

	startTs := float64(time.Now().UnixNano())/math.Pow(10, 9)

	storage.table.Range(copyRecord)

	err = writer.Flush()
	if err != nil {
		log.Printf("Can not make a checkpoint: %s\n", err)
		return
	}

	err = snapshotFile.Close()
	if err != nil {
		log.Printf("Can not make a checkpoint: %s\n", err)
		return
	}

	endTs := float64(time.Now().UnixNano())/math.Pow(10, 9)

	storage.Stats.CheckpointTs = append(storage.Stats.CheckpointTs, [2]float64{startTs, endTs})

	storage.logger.writeToDisk("end_checkpoint")

	saveCheckpoint(logPos, currentSnapshotFileName)

	log.Printf("%d records", recCount)
	log.Println("Checkpoint: end")
}

func saveCheckpoint(logPos int64, fileName string) {
	logPosFile, _ := os.OpenFile(lastCheckpointFileName, os.O_WRONLY | os.O_APPEND | os.O_CREATE, 0666)
	rec := fmt.Sprintf("%s\t%s\n", fileName, strconv.FormatInt(logPos, 10))
	logPosFile.Write([]byte(rec))
	logPosFile.Close()
}