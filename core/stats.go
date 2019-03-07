package core

import (
	"fmt"
	"github.com/robfig/cron"
	"log"
	"os"
	"strconv"
	"time"
)

type RWStats struct {
	readOp int
	writeOp int
	ts []int
	readQps []int
	writeQps []int

}

func (s *Storage) RunStats() {
	scheduler := cron.New()

	dumpToArray := func() {
		currentReadQps := s.Stats.readOp - sum(s.Stats.readQps)
		currentWriteQps := s.Stats.writeOp - sum(s.Stats.writeQps)

		s.Stats.ts = append(s.Stats.ts, int(time.Now().Unix()) - throughputWindowSize)
		s.Stats.readQps = append(s.Stats.readQps, currentReadQps)
		s.Stats.writeQps = append(s.Stats.writeQps, currentWriteQps)
	}

	err := scheduler.AddFunc(
	fmt.Sprintf("@every %ds", throughputWindowSize), dumpToArray)
	if err != nil {
	log.Fatalf("Can not run stats: %s", err)
	}

	scheduler.Start()

	s.statsScheduler = scheduler
}

func (stats RWStats) DumpToDisk() {
	fileName := fmt.Sprintf("%s_%s", statsFileName, strconv.FormatInt(time.Now().Unix(), 10))
	statsFile, err := os.Create(fileName)

	if err != nil {
		log.Println("Can not dump statistics to disk: %s", err)
	}

	defer statsFile.Close()

	backupFile, _ := os.OpenFile(fileName, os.O_WRONLY | os.O_CREATE, 0666)

	for i := range stats.writeQps {
		line := fmt.Sprintf("%d,%d,%d\n", stats.ts[i], stats.readQps[i], stats.writeQps[i])
		backupFile.Write([]byte(line))
	}
}
