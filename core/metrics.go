package core

import (
	"fmt"
	"github.com/robfig/cron"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
	"image/color"
	"log"
	"sort"
	"strconv"
	"time"
)

const (
	imageSizeVert = 25
	yAxisTicksCount = 35
)

func RunMetrics(s Storage) *cron.Cron {
	scheduler := cron.New()
	throughput := &s.Stats.Throughput

	updateMetric := func() {
		throughput.ts = append(throughput.ts, int(time.Now().Unix()))

		currentTotalRead := s.Stats.rwStats.readOp
		currentTotalWrite := s.Stats.rwStats.writeOp

		throughput.readQps = append(throughput.readQps, float64(currentTotalRead - throughput.prevTotalRead)/throughputWindowSize)
		throughput.writeQps = append(throughput.writeQps, float64(currentTotalWrite - throughput.prevTotalWrite)/throughputWindowSize)
		throughput.prevTotalRead = currentTotalRead
		throughput.prevTotalWrite = currentTotalWrite
	}

	err := scheduler.AddFunc(
		fmt.Sprintf("@every %ds", throughputWindowSize),
		updateMetric)
	if err != nil {
		log.Fatalf("Can not run stats: %s", err)
	}

	scheduler.Start()

	return scheduler
}

func SaveMetrics(s Storage) {
	p, err := plot.New()

	if err != nil {
		log.Printf("Can not show throughput metric: %s", err)
		return
	}

	throughput := &s.Stats.Throughput

	if len(throughput.ts) <= 1 {
		return
	}

	p.Title.Text = "DRReS throughput"
	p.Add(plotter.NewGrid())

	setXAxis(p, throughput.ts)
	setYAxis(p, throughput.readQps, throughput.writeQps)

	err = plotutil.AddLinePoints(p,
		"Read", buildPoints(throughput.ts, throughput.readQps),
		"Write", buildPoints(throughput.ts, throughput.writeQps))

	if err != nil {
		log.Printf("Can not show throughput metric: %s", err)
		return
	}

	maxYValue := getMaxYValue(throughput.readQps, throughput.writeQps)
	addCheckpointBars(p, s.Stats.CheckpointTs, maxYValue)

	p.Save(vg.Length(len(throughput.ts) - 1)*vg.Inch, imageSizeVert*vg.Inch, "throughput.png"); // horizontal size == number of 5 sec intervals
}

func setXAxis(p *plot.Plot, xs []int) {
	p.X.Label.Text = "ts"

	xTicks := make([]plot.Tick, len(xs))

	for i := range xTicks {
		xTicks[i] = plot.Tick{float64(xs[i]), strconv.Itoa(xs[i])}
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

func addCheckpointBars(p *plot.Plot, checkpointTs [][2]float64, yValue float64) {
	checkpointStartTs := make([]float64, len(checkpointTs))
	checkpointDuration := make([]float64, len(checkpointTs))

	for i := range checkpointTs {
		checkpointStartTs[i] = checkpointTs[i][0]
		checkpointDuration[i] = checkpointTs[i][1] - checkpointTs[i][0]
	}

	for i := range checkpointStartTs {
		barWidth := vg.Length(checkpointDuration[i]/5.0)*vg.Inch

		bar, _ := plotter.NewBarChart(plotter.Values{yValue}, barWidth)
		bar.XMin = float64(checkpointStartTs[i])
		bar.LineStyle.Width = vg.Length(0)
		bar.Color = plotutil.Color(2)

		p.Add(bar)
	}
}

func getMaxYValue(ys1 []float64, ys2 []float64) float64 {
	allY := append(ys1, ys2...)
	sort.Float64s(allY)
	return allY[len(allY) - 1]
}