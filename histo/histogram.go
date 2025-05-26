package histo

import (
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/aybabtme/uniplot/barchart"
	"github.com/aybabtme/uniplot/histogram"
)

var blocks = []string{
	"▏", "▎", "▍", "▌", "▋", "▊", "▉", "█",
}

var barstring = func(v float64) string {
	decimalf := (v - math.Floor(v)) * 10.0
	decimali := math.Floor(decimalf)
	charIdx := int(decimali / 10.0 * 8.0)
	return strings.Repeat("█", int(v)) + blocks[charIdx]
}

const MaxWidth = 80

func mapReverse[K comparable, V comparable](x map[K]V) map[V]K {
	m := make(map[V]K)
	for k, v := range x {
		m[v] = k
	}
	return m
}

func PrintHistogram(data []string, w io.Writer) error {
	n := 0
	labels := make(map[string]int)
	for _, s := range data {
		_, ok := labels[s]
		if !ok {
			labels[s] = n
			n++
		}
	}
	labelIds := mapReverse(labels)

	d := make([]float64, len(data))
	for i, s := range data {
		d[i] = float64(labels[s])
	}

	hist := histogram.Hist(len(labels), d)

	return printHistogramWithBucketName(w, hist, histogram.Linear(MaxWidth), labelIds)
}

func printHistogramWithBucketName(w io.Writer, h histogram.Histogram, s histogram.ScaleFunc, bucketNames map[int]string) error {
	tabw := tabwriter.NewWriter(w, 2, 2, 2, byte(' '), 0)

	yfmt := func(y int) string {
		if y > 0 {
			return strconv.Itoa(y)
		}
		return ""
	}

	var err error

	for i, bkt := range h.Buckets {
		sz := h.Scale(s, i)
		_, err = fmt.Fprintf(tabw, "%s\t%.3g%%\t%s\n",
			bucketNames[i], float64(bkt.Count)*100.0/float64(h.Count),
			barstring(sz)+"\t"+yfmt(bkt.Count),
		)
		if err != nil {
			return err
		}
	}

	return tabw.Flush()
}

func PrintBarChart(d map[string]float64, w io.Writer) error {
	n := 0
	series := make(map[string]int)
	for k := range d {
		_, ok := series[k]
		if !ok {
			series[k] = n
			n++
		}
	}
	idSeries := mapReverse(series)

	data := make([][2]int, len(series))
	for i := range data {
		data[i] = [2]int{i, 0}
	}

	for k, v := range d {
		id := series[k]
		data[id][1] += int(v)
	}

	plots := barchart.BarChartXYs(data)
	width := plots.MaxX - plots.MinX + 1
	return barchart.Fprintf(w, plots, width, barchart.Linear(MaxWidth),
		func(v float64) string { return idSeries[int(v)] },
		func(v float64) string { return fmt.Sprintf("%v", v) })
}

func SkipZeroPrintf(w io.Writer, h histogram.Histogram, s histogram.ScaleFunc, f histogram.FormatFunc) error {
	tabw := tabwriter.NewWriter(w, 2, 2, 2, byte(' '), 0)

	yfmt := func(y int) string {
		if y > 0 {
			return strconv.Itoa(y)
		}
		return ""
	}

	for i, bkt := range h.Buckets {
		if bkt.Count <= 0 {
			continue
		}
		sz := h.Scale(s, i)
		fmt.Fprintf(tabw, "%s-%s\t%.3g%%\t%s\n",
			f(bkt.Min), f(bkt.Max),
			float64(bkt.Count)*100.0/float64(h.Count),
			barstring(sz)+"\t"+yfmt(bkt.Count),
		)
	}

	return tabw.Flush()
}
