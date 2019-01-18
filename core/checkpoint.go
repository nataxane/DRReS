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
	f, err := os.OpenFile(snapshotFileName, os.O_WRONLY | os.O_CREATE, 0666)
	if err != nil {
		log.Printf("Can not make a checkpoint: %s", err)
	}

	copyRecord := func(key, value interface{}) bool {
		f.Write([]byte(fmt.Sprintf("%s\t%s\n", key, value)))
		return true
	}

	storage.logger.writeToDisk("begin_checkpoint")

	for _, table := range storage.tables {
		table.Range(copyRecord)
		f.Sync()
	}

	storage.logger.writeToDisk("end_checkpoint")
}