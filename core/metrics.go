package core

import (
	"fmt"
	"github.com/robfig/cron"
	"log"
	"time"
)

type Metric struct {
	ts []int64
	readQps []float64
	writeQps []float64

	prevTotalRead int
	prevTotalWrite int
}

func RunStats(s Storage) (*cron.Cron, Metric) {
	scheduler := cron.New()

	metric := Metric{}

	updateStats := func() {
		metric.ts = append(metric.ts, time.Now().Unix())

		currentTotalRead := s.stats.readOp
		currentTotalWrite := s.stats.writeOp

		metric.readQps = append(metric.readQps, float64(currentTotalRead - metric.prevTotalRead)/throughputWindowSize)
		metric.writeQps = append(metric.writeQps, float64(currentTotalWrite - metric.prevTotalWrite)/throughputWindowSize)
		metric.prevTotalRead = currentTotalRead
		metric.prevTotalWrite = currentTotalWrite

		metric.ShowPlot()
	}

	err := scheduler.AddFunc(
		fmt.Sprintf("@every %ds", throughputWindowSize),
		updateStats)
	if err != nil {
		log.Fatalf("Can not run stats: %s", err)
	}

	scheduler.Start()

	return scheduler, metric
}


func (m Metric) ShowPlot() {
	// dummy
	fmt.Println(m.ts)
	fmt.Println(m.readQps)
	fmt.Println(m.writeQps)
}
