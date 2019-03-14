package core

import (
	"fmt"
	"github.com/robfig/cron"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
)

func RunCompactionScheduler(checkpointCompactionSync *sync.WaitGroup) *cron.Cron {
	scheduler := cron.New()

	compactionFreq := cpFreq*keepLastNCheckpoints + cpFreq*0.5

	err := scheduler.AddFunc(
		fmt.Sprintf("@every %ds", int(compactionFreq)),
		func() {doCompaction(checkpointCompactionSync)})

	if err != nil {
		log.Printf("Can not run compaction: %s", err)
	}

	scheduler.Start()

	return scheduler
}


func compactLastCheckpointFile(checkpointList []string) error {
	lastCheckpointFile, err := os.OpenFile(lastCheckpointFileName, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, 0666)
	if err != nil {
		return err
	}

	defer lastCheckpointFile.Close()

	_, err = lastCheckpointFile.Write([]byte(strings.Join(checkpointList, "\n") + "\n"))
	if err != nil {
		return err
	}

	return nil
}

func compactSnapshotDir(checkpointList []string) error {
	snapshotsToKeep := make(map[string]int)

	for i := range checkpointList {
		checkpointInfo := strings.Split(checkpointList[i], "\t")
		fmt.Println(checkpointInfo)
		snapshotsToKeep[checkpointInfo[1]] = 1
	}

	snapshotFiles, err := ioutil.ReadDir(snapshotDir)
	if err != nil {
		return err
	}

	for _, file := range snapshotFiles {
		path := fmt.Sprintf("%s/%s", snapshotDir, file.Name())
		_, ok := snapshotsToKeep[path]

		if ok == false {
			err = os.Remove(path)
			if err != nil {
				log.Printf("Can not remove %s snapshot: %v", file.Name(), err)
			}
		}
	}

	return nil
}

func doCompaction(checkpointCompactionSync *sync.WaitGroup) {
	checkpointCompactionSync.Wait()
	checkpointCompactionSync.Add(1)

	log.Printf("Compaction started")

	checkpointList, err := getCheckpointList()
	if err != nil {
		log.Printf("Skip compaction: %v", err)
		return
	}

	if len(checkpointList) <= keepLastNCheckpoints {
		log.Println("Skip compaction: nothing to compact")
		return
	}

	checkpointListTruncated := checkpointList[len(checkpointList)-5:]

	err = compactLastCheckpointFile(checkpointListTruncated)
	if err != nil {
		log.Printf("Skip compaction: %v", err)
		return
	}

	err = compactSnapshotDir(checkpointListTruncated)
	if err != nil {
		log.Printf("Skip compaction: %v", err)
		return
	}

	log.Printf("Compaction finished")
	checkpointCompactionSync.Done()
}