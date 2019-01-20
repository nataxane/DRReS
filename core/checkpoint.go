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

func (s Storage) RunCheckpointing() *cron.Cron {
	scheduler := cron.New()

	err := scheduler.AddFunc("@every 1m", func() {makeCheckpoint(s)})
	if err != nil {
		log.Fatalf("Can not run checkpointing scheduler: %s", err)
	}

	scheduler.Start()

	return scheduler
}

func makeCheckpoint(storage Storage) {
	currentSnapshotFileName := fmt.Sprintf("%s/%s", snapshotDir, strconv.FormatInt(time.Now().Unix(), 10))
	snapshotFile, err := os.OpenFile(currentSnapshotFileName, os.O_WRONLY | os.O_CREATE, 0666)

	if err != nil {
		log.Printf("Can not make a checkpoint: %s\n", err)
		return
	}

	writer := bufio.NewWriter(snapshotFile)

	copyRecord := func(key, value interface{}) bool {
		_, err = writer.Write([]byte(fmt.Sprintf("%s\t%s\n", key, value)))

		if err != nil {
			log.Panicln(err)
		}

		return true
	}

	logPos := storage.logger.writeToDisk("begin_checkpoint")
	log.Println("Checkpoint: start")

	storage.table.Range(copyRecord)

	err = snapshotFile.Sync()
	if err != nil {
		log.Printf("Can not make a checkpoint: %s\n", err)
		return
	}

	err = snapshotFile.Close()
	if err != nil {
		log.Printf("Can not make a checkpoint: %s\n", err)
		return
	}

	storage.logger.writeToDisk("end_checkpoint")

	saveCheckpoint(logPos, currentSnapshotFileName)

	log.Println("Checkpoint: end")
}

func saveCheckpoint(logPos int64, fileName string) {
	logPosFile, _ := os.OpenFile(lastCheckpointFileName, os.O_WRONLY | os.O_APPEND | os.O_CREATE, 0666)
	rec := fmt.Sprintf("%s\t%s\n", fileName, strconv.FormatInt(logPos, 10))
	logPosFile.Write([]byte(rec))
	logPosFile.Close()
}