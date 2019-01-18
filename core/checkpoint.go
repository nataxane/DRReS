package core

import (
	"fmt"
	"github.com/robfig/cron"
	"log"
	"os"
)

func (s Storage) RunCheckpointing() *cron.Cron {
	scheduler := cron.New()

	err := scheduler.AddFunc("@every 5s", func() {makeCheckpoint(s)})
	if err != nil {
		log.Fatalf("Can not run checkpointing scheduler: %s", err)
	}

	scheduler.Start()

	return scheduler
}

func makeCheckpoint(storage Storage) {
	snapshotFile, snapshotErr := os.OpenFile(snapshotFileName, os.O_WRONLY | os.O_CREATE, 0666)
	logPosFile, logPosErr := os.OpenFile(lastCheckpointFileName, os.O_WRONLY | os.O_CREATE, 0666)

	if snapshotErr != nil {
		log.Printf("Can not make a checkpoint: %s", snapshotErr)
	}
	if snapshotErr != nil {
		log.Printf("Can not make a checkpoint: %s", logPosErr)
	}

	copyRecord := func(key, value interface{}) bool {
		snapshotFile.Write([]byte(fmt.Sprintf("%s\t%s\n", key, value)))
		return true
	}

	logPos := storage.logger.writeToDisk("begin_checkpoint")
	log.Printf("Checkpoint: start. Log position; %v\n", logPos)

	storage.table.Range(copyRecord)
	snapshotFile.Sync()

	storage.logger.writeToDisk("end_checkpoint")

	logPosFile.Write([]byte(string(logPos)))

	log.Println("Checkpoint: end")
}