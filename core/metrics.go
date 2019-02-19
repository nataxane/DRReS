package core

import (
	"fmt"
	"github.com/robfig/cron"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
	"log"
	"sort"
	"strconv"
	"time"
)

const (
	imageSize = 14
	xAxisTicksCount = 15
	yAxisTicksCount = 20
)

type Metric struct {
	ts []int
	readQps []float64
	writeQps []float64

	prevTotalRead int
	prevTotalWrite int
}

func RunStats(s Storage) (*cron.Cron, *Metric) {
	scheduler := cron.New()

	metric := Metric{}

	updateStats := func() {
		metric.ts = append(metric.ts, int(time.Now().Unix()))

		currentTotalRead := s.stats.readOp
		currentTotalWrite := s.stats.writeOp

		metric.readQps = append(metric.readQps, float64(currentTotalRead - metric.prevTotalRead)/throughputWindowSize)
		metric.writeQps = append(metric.writeQps, float64(currentTotalWrite - metric.prevTotalWrite)/throughputWindowSize)
		metric.prevTotalRead = currentTotalRead
		metric.prevTotalWrite = currentTotalWrite
	}

	err := scheduler.AddFunc(
		fmt.Sprintf("@every %ds", throughputWindowSize),
		updateStats)
	if err != nil {
		log.Fatalf("Can not run stats: %s", err)
	}

	scheduler.Start()

	return scheduler, &metric
}

func (m Metric) SavePlot() {
	p, err := plot.New()

	if err != nil {
		log.Printf("Can not show throughput metric: %s", err)
		return
	}

	p.Title.Text = "DRReS throughput"
	setXAxis(p, m.ts)
	setYAxis(p, m.readQps, m.writeQps)

	err = plotutil.AddLinePoints(p,
		"Read", buildPoints(m.ts, m.readQps),
		"Write", buildPoints(m.ts, m.writeQps))

	if err != nil {
		log.Printf("Can not show throughput metric: %s", err)
		return
	}

	p.Save(imageSize*vg.Inch, imageSize*vg.Inch, "throughput.png");
}

func setXAxis(p *plot.Plot, xs []int) {
	p.X.Label.Text = "ts"

	xTicks := make([]plot.Tick, xAxisTicksCount)

	xRange := xs[len(xs) - 1] - xs[0]
	delta := xRange / (xAxisTicksCount - 1)

	for i := range xTicks {
		tick := xs[0] + delta * i
		xTicks[i] = plot.Tick{float64(tick), strconv.Itoa(tick)}
	}
	p.X.Tick.Marker = plot.ConstantTicks(xTicks)
}

func setYAxis(p *plot.Plot, ys1 []float64, ys2 []float64) {
	p.Y.Label.Text = "qps"

	yTicks := make([]plot.Tick, yAxisTicksCount)

	allY := append(ys1, ys2...)
	sort.Float64s(allY)

	yRange := allY[len(allY) - 1] - allY[0]
	delta := yRange / (yAxisTicksCount - 1)

	for i := range yTicks {
		tick := allY[0] + delta * float64(i)
		yTicks[i] = plot.Tick{float64(tick), strconv.Itoa(int(tick))}
	}
	p.Y.Tick.Marker = plot.ConstantTicks(yTicks)
}

func buildPoints(x []int, y []float64) plotter.XYs {
	pts := make(plotter.XYs, len(x))

	for i := range x {
		pts[i].X = float64(x[i])
		pts[i].Y = y[i]
	}

	return pts
}