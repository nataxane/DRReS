package core

import (
	"fmt"
	"github.com/robfig/cron"
	"log"
	"os"
)

func (storage Storage) RunCheckpointing() *cron.Cron {
	scheduler := cron.New()

	err := scheduler.AddFunc("@every 5s", func() {makeCheckpoint(storage)})
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

	storage.logger.writeToDisk("begin_checkpoint")

	for _, table := range storage.tables {
		storage.locker.Lock()
		for key, value := range table {
			fmt.Printf("%s\t%s\n", key, value)
			f.Write([]byte(fmt.Sprintf("%s\t%s\n", key, value)))
		}
		storage.locker.Unlock()
	}

	f.Sync()

	storage.logger.writeToDisk("end_checkpoint")
}